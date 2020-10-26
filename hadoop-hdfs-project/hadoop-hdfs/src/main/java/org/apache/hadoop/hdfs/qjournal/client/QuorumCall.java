/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.qjournal.client;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.Timer;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;


/**
 * Represents a set of calls for which a quorum of results is needed.
 * @param <KEY> a key used to identify each of the outgoing calls
 * @param <RESULT> the type of the call result
 */
class QuorumCall<KEY, RESULT> {
  private final Map<KEY, RESULT> successes = Maps.newHashMap();
  private final Map<KEY, Throwable> exceptions = Maps.newHashMap();

  /**
   * Interval, in milliseconds, at which a log message will be made
   * while waiting for a quorum call.
   */
  private static final int WAIT_PROGRESS_INTERVAL_MILLIS = 1000;

  /**
   * Start logging messages at INFO level periodically after waiting for
   * this fraction of the configured timeout for any call.
   */
  private static final float WAIT_PROGRESS_INFO_THRESHOLD = 0.3f;
  /**
   * Start logging messages at WARN level after waiting for this
   * fraction of the configured timeout for any call.
   */
  private static final float WAIT_PROGRESS_WARN_THRESHOLD = 0.7f;
  private final StopWatch quorumStopWatch;
  private final Timer timer;

  static <KEY, RESULT> QuorumCall<KEY, RESULT> create(
      Map<KEY, ? extends ListenableFuture<RESULT>> calls, Timer timer) {
    final QuorumCall<KEY, RESULT> qr = new QuorumCall<KEY, RESULT>(timer);
    for (final Entry<KEY, ? extends ListenableFuture<RESULT>> e : calls.entrySet()) {
      Preconditions.checkArgument(e.getValue() != null,
          "null future for key: " + e.getKey());
      Futures.addCallback(e.getValue(), new FutureCallback<RESULT>() {
        @Override
        public void onFailure(Throwable t) {
          qr.addException(e.getKey(), t);
        }

        @Override
        public void onSuccess(RESULT res) {
          qr.addResult(e.getKey(), res);
        }
      }, MoreExecutors.directExecutor());
    }
    return qr;
  }

  static <KEY, RESULT> QuorumCall<KEY, RESULT> create(
      Map<KEY, ? extends ListenableFuture<RESULT>> calls) {
    return create(calls, new Timer());
  }

  /**
   * Not intended for outside use.
   */
  private QuorumCall() {
    this(new Timer());
  }

  private QuorumCall(Timer timer) {
    // Only instantiated from factory method above
    this.timer = timer;
    this.quorumStopWatch = new StopWatch(timer);
  }

  /**
   * Used in conjunction with {@link #getQuorumTimeoutIncreaseMillis(long, int)}
   * to check for pauses.
   */
  private void restartQuorumStopWatch() {
    quorumStopWatch.reset().start();
  }

  /**
   * Check for a pause (e.g. GC) since the last time
   * {@link #restartQuorumStopWatch()} was called. If detected, return the
   * length of the pause; else, -1.
   * @param offset Offset the elapsed time by this amount; use if some amount
   *               of pause was expected
   * @param millis Total length of timeout in milliseconds
   * @return Length of pause, if detected, else -1
   */
  private long getQuorumTimeoutIncreaseMillis(long offset, int millis) {
    // elapsed计时器计算的时间
    long elapsed = quorumStopWatch.now(TimeUnit.MILLISECONDS);
    // 停顿时间
    long pauseTime = elapsed + offset;
    // 这里表示如果停顿时间大于超时时间的1/3，就认为是发生了FullGC，返回停顿时间
    // 否则返回-1
    if (pauseTime > (millis * WAIT_PROGRESS_INFO_THRESHOLD)) {
      QuorumJournalManager.LOG.info("Pause detected while waiting for " +
          "QuorumCall response; increasing timeout threshold by pause time " +
          "of " + pauseTime + " ms.");
      return pauseTime;
    } else {
      return -1;
    }
  }


  /**
   * Wait for the quorum to achieve a certain number of responses.
   *
   * Note that, even after this returns, more responses may arrive,
   * causing the return value of other methods in this class to change.
   *
   * @param minResponses return as soon as this many responses have been
   * received, regardless of whether they are successes or exceptions
   * @param minSuccesses return as soon as this many successful (non-exception)
   * responses have been received
   * @param maxExceptions return as soon as this many exception responses
   * have been received. Pass 0 to return immediately if any exception is
   * received.
   * @param millis the number of milliseconds to wait for
   * @throws InterruptedException if the thread is interrupted while waiting
   * @throws TimeoutException if the specified timeout elapses before
   * achieving the desired conditions
   */
  public synchronized void waitFor(
      int minResponses, int minSuccesses, int maxExceptions,
      int millis, String operationName)
      throws InterruptedException, TimeoutException {
    // minResponses：最少已经返回响应的数量，这里就等于AsyncLogger的数量，就是JournalNode数量
    // minSuccesses：最少已经成功响应的数量，大多数节点的数量
    // maxExceptions：最多响应异常的数量，大多数节点的数量
    // millis：超时时间，默认是20s
    // operationName：sendEdits

    // 先模拟一下程序执行过程，假设第一遍进入循环正常执行，第二遍进入循环后在循环内几个地方发生了FullGC

    // st：开始执行时间
    // 1、假设是10:00:00
    long st = timer.monotonicNow();
    // 下次打印log时间是：超时时间 * 0.3，默认就是6s
    // 1、根据开始时间计算为10:00:06
    long nextLogTime = st + (long)(millis * WAIT_PROGRESS_INFO_THRESHOLD);
    // et：结束执行时间
    // 1、根据开始时间计算为10:00:20
    long et = st + millis;
    while (true) {
      // 重置StopWatch计时器
      // TODO：这里有个疑问，如果在重置StopWatch的代码执行前，计时器还没开始计时
      //  此时发生了FullGC导致停顿，计时器就不起作用了，理论上还是存在一定概率FullGC导致QJM集群写入失败后异常宕机？
      restartQuorumStopWatch();
      checkAssertionErrors();
      // 如果返回响应的节点数达到了最少响应数量，则返回
      if (minResponses > 0 && countResponses() >= minResponses) return;
      // 如果最少成功响应的数量达到，则返回
      if (minSuccesses > 0 && countSuccesses() >= minSuccesses) return;
      // 如果异常响应的数量达到最多，则返回
      if (maxExceptions >= 0 && countExceptions() > maxExceptions) return;
      // 当前时间
      // 1、假设执行完上面这段代码后时间变为（10:00:01）
      // 2、假设这里发生FullGC停顿了30s（如果NameNode内存很大，FullGC甚至可能长达几分钟）,此时时间为（10:00:37）
      long now = timer.monotonicNow();

      // 这里是打印日志的，就是等待超过一定时间就会打印一些日志
      // 1、这里不会进
      // 2、这里会进分支
      if (now > nextLogTime) {
        long waited = now - st;
        String msg = String.format(
            "Waited %s ms (timeout=%s ms) for a response for %s",
            waited, millis, operationName);
        if (!successes.isEmpty()) {
          msg += ". Succeeded so far: [" + Joiner.on(",").join(successes.keySet()) + "]";
        }
        if (!exceptions.isEmpty()) {
          msg += ". Exceptions so far: [" + getExceptionMapString() + "]";
        }
        if (successes.isEmpty() && exceptions.isEmpty()) {
          msg += ". No responses yet.";
        }
        if (waited > millis * WAIT_PROGRESS_WARN_THRESHOLD) {
          QuorumJournalManager.LOG.warn(msg);
        } else {
          QuorumJournalManager.LOG.info(msg);
        }
        // 2、nextLogTime设置为（10:00:38）
        nextLogTime = now + WAIT_PROGRESS_INTERVAL_MILLIS;
      }

      // rem：结束时间距离当前时间还剩多少时间
      // 1、19s
      // 2、-18s
      long rem = et - now;
      // 小于等于0表示结束时间已经到了
      // 1、不会进这个条件分支
      // 2、会进这个条件分支
      if (rem <= 0) {
        // Increase timeout if a full GC occurred after restarting stopWatch
        // 这里是获取计时器计算的时间，就是表示FullGC停顿的时间
        // 2、计算停顿时间为30s
        long timeoutIncrease = getQuorumTimeoutIncreaseMillis(0, millis);
        // 如果停顿时间大于0，表示触发了FullGC，et则顺延相应GC停顿的时间
        // 2、会进这个分支，将et顺延30s（10:00:50），程序可以继续执行
        if (timeoutIncrease > 0) {
          et += timeoutIncrease;
        } else {
          // 否则则认为是写入失败，抛出异常，异常一层一层往外抛，最后进程会退出
          throw new TimeoutException();
        }
      }
      // 重置StopWatch计时器
      // TODO：这里同上
      restartQuorumStopWatch();
      // 1、rem = 5s
      // 2、rem = -18s
      rem = Math.min(rem, nextLogTime - now);
      // 1、rem = 5s
      // 2、rem = 1s
      rem = Math.max(rem, 1);
      // 1、wait等待5s
      // 2、wait等待1s
      wait(rem);
      // Increase timeout if a full GC occurred after restarting stopWatch
      // 1、这里获取的停顿时间为-1
      // 2、假设在重置计时器和执行行代码之前，发生了FullGC，时间为30s，也会将et顺延30s
      //    不然下次进入循环后当前时间就已经超过了et，同样会导致异常。
      long timeoutIncrease = getQuorumTimeoutIncreaseMillis(-rem, millis);
      // 1、不会进这个条件分支
      if (timeoutIncrease > 0) {
        et += timeoutIncrease;
      }
    }
  }

  /**
   * Check if any of the responses came back with an AssertionError.
   * If so, it re-throws it, even if there was a quorum of responses.
   * This code only runs if assertions are enabled for this class,
   * otherwise it should JIT itself away.
   *
   * This is done since AssertionError indicates programmer confusion
   * rather than some kind of expected issue, and thus in the context
   * of test cases we'd like to actually fail the test case instead of
   * continuing through.
   */
  private synchronized void checkAssertionErrors() {
    boolean assertsEnabled = false;
    assert assertsEnabled = true; // sets to true if enabled
    if (assertsEnabled) {
      for (Throwable t : exceptions.values()) {
        if (t instanceof AssertionError) {
          throw (AssertionError)t;
        } else if (t instanceof RemoteException &&
            ((RemoteException)t).getClassName().equals(
                AssertionError.class.getName())) {
          throw new AssertionError(t);
        }
      }
    }
  }

  private synchronized void addResult(KEY k, RESULT res) {
    successes.put(k, res);
    notifyAll();
  }

  private synchronized void addException(KEY k, Throwable t) {
    exceptions.put(k, t);
    notifyAll();
  }

  /**
   * @return the total number of calls for which a response has been received,
   * regardless of whether it threw an exception or returned a successful
   * result.
   */
  public synchronized int countResponses() {
    return successes.size() + exceptions.size();
  }

  /**
   * @return the number of calls for which a non-exception response has been
   * received.
   */
  public synchronized int countSuccesses() {
    return successes.size();
  }

  /**
   * @return the number of calls for which an exception response has been
   * received.
   */
  public synchronized int countExceptions() {
    return exceptions.size();
  }

  /**
   * @return the map of successful responses. A copy is made such that this
   * map will not be further mutated, even if further results arrive for the
   * quorum.
   */
  public synchronized Map<KEY, RESULT> getResults() {
    return Maps.newHashMap(successes);
  }

  public synchronized void rethrowException(String msg) throws QuorumException {
    Preconditions.checkState(!exceptions.isEmpty());
    throw QuorumException.create(msg, successes, exceptions);
  }

  public static <K> String mapToString(
      Map<K, ? extends Message> map) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<K, ? extends Message> e : map.entrySet()) {
      if (!first) {
        sb.append("\n");
      }
      first = false;
      sb.append(e.getKey()).append(": ")
        .append(TextFormat.shortDebugString(e.getValue()));
    }
    return sb.toString();
  }

  /**
   * Return a string suitable for displaying to the user, containing
   * any exceptions that have been received so far.
   */
  private String getExceptionMapString() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (Map.Entry<KEY, Throwable> e : exceptions.entrySet()) {
      if (!first) {
        sb.append(", ");
      }
      first = false;
      sb.append(e.getKey()).append(": ")
        .append(e.getValue().getLocalizedMessage());
    }
    return sb.toString();
  }
}
