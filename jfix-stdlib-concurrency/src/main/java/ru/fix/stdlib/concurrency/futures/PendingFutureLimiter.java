package ru.fix.stdlib.concurrency.futures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.dynamic.property.api.PropertySubscription;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Limit number of scheduled async operations.
 * Used to create backpressure for producer.
 * <p>
 * maxPendingCount - maximum limit of futures within {@link PendingFutureLimiter}
 * maxFutureExecuteTimeout - if 0 then this option is ignored.
 *                        How long to wait for future to complete.
 *                        If future is not complete during this timeout, then future will be removed from waiting queue.
 *                        And resulting future returned from {@link #enqueueBlocking(CompletableFuture)}
 *                        or {@link #enqueueUnlimited(CompletableFuture)} will complete exceptionally.
 *                        If it set to 0, futures in the queue will always await for completion
 *                        before being  removed from the queue.
 * </p>
 */
public class PendingFutureLimiter {
    private final Logger log = LoggerFactory.getLogger(PendingFutureLimiter.class);

    private final AtomicLong counter = new AtomicLong();
    /**
     * Pending futures and time of their registration within {@link PendingFutureLimiter}
     */
    private final ConcurrentHashMap<CompletableFuture<?>, Long> pendingFutures = new ConcurrentHashMap<>();
    private final PropertySubscription<Integer> maxPendingCountSubscription;
    private final PropertySubscription<Long> maxFutureExecuteTimeoutSubscription;

    private volatile int maxPendingCount = 100;
    private volatile float thresholdFactor = 0.3f;
    private volatile long maxFutureExecuteTime = 0;
    private volatile long pendingQueueSizeChangeCheckInterval = TimeUnit.SECONDS.toMillis(10);

    private ThresholdListener thresholdListener;

    /**
     * @param maxPendingCount         max amount of {@link CompletableFuture}s started in parallel
     * @param maxFutureExecuteTimeout maximum time (milliseconds) to wait for {@link CompletableFuture} to complete.
     */
    public PendingFutureLimiter(int maxPendingCount, long maxFutureExecuteTimeout) {
        this.maxPendingCount = maxPendingCount;
        this.maxFutureExecuteTime = maxFutureExecuteTimeout;
        this.maxPendingCountSubscription = null;
        this.maxFutureExecuteTimeoutSubscription = null;
    }

    /**
     * @param maxPendingCount         max amount of {@link CompletableFuture}s started in parallel
     * @param maxFutureExecuteTimeout maximum time (milliseconds) on {@link CompletableFuture} to complete.
     */
    public PendingFutureLimiter(DynamicProperty<Integer> maxPendingCount, DynamicProperty<Long> maxFutureExecuteTimeout) {
        this.maxPendingCountSubscription = maxPendingCount
                .createSubscription()
                .setAndCallListener((oldValue, newValue) -> setMaxPendingCount(newValue));

        this.maxFutureExecuteTimeoutSubscription = maxFutureExecuteTimeout
                .createSubscription()
                .setAndCallListener((oldValue, newValue) -> setMaxFutureExecuteTime(newValue));
    }

    private static int calculateThreshold(int maxPendingCount, float thresholdFactor) {
        return (int) (maxPendingCount * (1 - thresholdFactor));
    }

    protected int getThreshold() {
        return calculateThreshold(maxPendingCount, thresholdFactor);
    }

    protected int getMaxPendingCount() {
        return maxPendingCount;
    }

    public void setPendingQueueSizeChangeCheckInterval(long pendingQueueSizeChangeCheckInterval) {
        this.pendingQueueSizeChangeCheckInterval = pendingQueueSizeChangeCheckInterval;
    }

    /**
     * Change pending futures maximum amount
     */
    public PendingFutureLimiter setMaxPendingCount(int maxPendingCount) {

        int threshold = calculateThreshold(maxPendingCount, thresholdFactor);

        if (threshold <= 0 || threshold >= maxPendingCount) {
            throw new IllegalArgumentException("Invalid thresholdFactor");
        }

        this.maxPendingCount = maxPendingCount;

        synchronized (counter) {
            counter.notifyAll();
        }

        return this;
    }

    public float getThresholdFactor() {
        return thresholdFactor;
    }

    public void setMaxFutureExecuteTime(long maxFutureExecuteTime) {
        this.maxFutureExecuteTime = maxFutureExecuteTime;
    }

    public void setThresholdListener(ThresholdListener thresholdListener) {
        this.thresholdListener = thresholdListener;
    }

    /**
     * Sets the new value for thresholdFactor.
     * <p/>
     * ThresholdFactor is a ratio of free slots to maxPendingCount,
     * When it is reached callback {@link ThresholdListener#onLowLimitSubceed()} will be activated.
     * I. e., to get a threshold 800 with maxPendingCount 1000,
     * you should set thresholdFactor = 0.2
     *
     * @param thresholdFactor % of free slots from maxPendingCount,
     *                        when it is reached the signal about critical workload will be sent
     *                        to {@link ThresholdListener#onLowLimitSubceed()}
     */
    public PendingFutureLimiter changeThresholdFactor(float thresholdFactor) {

        int threshold = calculateThreshold(maxPendingCount, thresholdFactor);

        if (threshold <= 0 || threshold >= maxPendingCount) {
            throw new IllegalArgumentException("Invalid thresholdFactor");
        }

        this.thresholdFactor = thresholdFactor;

        synchronized (counter) {
            counter.notifyAll();
        }

        return this;
    }

    /**
     * Unlimited adding of futures to the queue.
     * Does not block.
     */
    public <T> CompletableFuture<T> enqueueUnlimited(CompletableFuture<T> future) throws InterruptedException {
        return internalEnqueue(future, false);
    }

    /**
     * Limited adding of the future to the queue.
     * If the queue has already reached maxPendingCount then block invoking thread till the queue releases.
     * Then, if maxFutureExecuteTime > 0, by reaching this timeout in milliseconds,
     * future will be removed from the queue and conditions to add ew features are created
     * If maxFutureExecuteTime = 0, the queue will be not released by timeout and the providing futures thread
     * will be blocked, till the futures in the queue will be completed
     */
    public <T> CompletableFuture<T> enqueueBlocking(CompletableFuture<T> future) throws InterruptedException {
        return internalEnqueue(future, true);
    }

    /**
     * Registers future to the inner counter.
     * Subscribes on the future listener which decrements counter in case of future completion.
     * Blocks - if the current counter value reaches or surpasses the maxPendingCount
     * Unblocks - if pending futures amount = maxPendingCount * thresholdFactor
     * Unblocking takes place in the method, started in ForkJoinPool after future will be completed
     * When thresholdListener is set, its methods invoked:
     * ThresholdListener::onLimitReached - before blocking
     * ThresholdListener::onLimitSubceed - after unblocking
     */
    protected <T> CompletableFuture<T> internalEnqueue(CompletableFuture<T> future,
                                                       boolean needToBlock) throws InterruptedException {
        if (counter.get() == maxPendingCount && thresholdListener != null) {
            thresholdListener.onHiLimitReached();
        }

        if (needToBlock) {
            awaitOpportunityToEnqueueAndPurge();
        }

        CompletableFuture<T> resultFuture = future.handleAsync((any, exc) -> {
            if (exc != null) {
                log.error(exc.getMessage(), exc);
                throw new CompletionException(exc);
            } else {
                return any;
            }
        });

        resultFuture.handleAsync((any, exc) -> {
            long value = counter.decrementAndGet();
            pendingFutures.remove(future);

            if (value == 0 || value == getThreshold()) {
                if (thresholdListener != null) {
                    thresholdListener.onLowLimitSubceed();
                }
                synchronized (counter) {
                    counter.notifyAll();
                }
            }
            return null;
        });
        counter.incrementAndGet();
        pendingFutures.put(resultFuture, System.currentTimeMillis());
        return resultFuture;
    }

    private void awaitOpportunityToEnqueueAndPurge() throws InterruptedException {
        while (counter.get() >= maxPendingCount) {
            synchronized (counter) {
                if (counter.get() >= maxPendingCount)
                    counter.wait(pendingQueueSizeChangeCheckInterval);
            }
            releaseTimeoutedIfPossible();
        }
    }

    public long getPendingCount() {
        return counter.get();
    }

    /**
     * wait all futures to complete up to timeoutMillis milliseconds
     * If maxFutureExecuteTime is set to 0 it will block invoking thread up to timeoutMillis, till all futures are complete
     * If maxFutureExecuteTime > 0 then it will block invoking thread up to timeoutMillis, till all futures are complete or reach maxFutureExecuteTime
     *
     * @param timeoutMillis maximum time to block invoking thread
     * @return true, if all threads where completed or reached maxFutureExecuteTime in timeoutMillis milliseconds
     * false, if some future(s) remained uncompleted and didn't reach maxFutureExecuteTime in timeoutMillis milliseconds.
     * In this case, uncompleted futures will remain in the queue.
     */
    public boolean waitAll(long timeoutMillis) throws InterruptedException {
        long start = System.currentTimeMillis();

        while (counter.get() > 0) {
            releaseTimeoutedIfPossible();
            if (System.currentTimeMillis() - start > timeoutMillis && counter.get() > 0) {
                Throwable t = (new Throwable("Waiting pending futures to complete failed in " + timeoutMillis + " milliseconds. "
                        + counter.get() + " futures remain in the queue."));
                log.error(t.getMessage(), t);
                return false;
            }
            if (counter.get() > 0) {
                long timeToWait = Math.min(pendingQueueSizeChangeCheckInterval, timeoutMillis - (System.currentTimeMillis() - start));
                if (timeToWait > 0) {
                    synchronized (counter) {
                        if (counter.get() > 0) {
                            counter.wait(timeToWait);
                        }
                    }
                }
            }
        }
        return true;

    }

    /**
     * Wait all futures to complete
     * If maxFutureExecuteTime is set to 0 it will block invoking thread till all futures are complete
     * If maxFutureExecuteTime > 0 then it will block invoking thread till all futures are complete or reach maxFutureExecuteTime
     * execution time limit
     */
    public void waitAll() throws InterruptedException {
        while (counter.get() > 0) {
            releaseTimeoutedIfPossible();
            if (counter.get() > 0) {
                synchronized (counter) {
                    if (counter.get() > 0)
                        counter.wait(pendingQueueSizeChangeCheckInterval);
                }
            }
        }
    }

    private void releaseTimeoutedIfPossible() {
        if (maxFutureExecuteTime == 0) {
            return;
        }

        String errorMessage = "Timeout exception. Completable future did not complete for at least "
                + maxFutureExecuteTime + " milliseconds. Pending count: " + getPendingCount();

        Consumer<Map.Entry<CompletableFuture<?>, Long>> completeExceptionally =
                entry -> entry.getKey().completeExceptionally(new TimeoutException(errorMessage));

        Predicate<Map.Entry<CompletableFuture<?>, Long>> isTimeoutPredicate =
                entry -> System.currentTimeMillis() - entry.getValue() > maxFutureExecuteTime;

        pendingFutures.entrySet().stream().filter(isTimeoutPredicate).forEach(completeExceptionally);

        if (pendingFutures.entrySet().stream().anyMatch(isTimeoutPredicate)) {
            log.error(errorMessage);
        }
    }


    public interface ThresholdListener {
        /**
         * Called, whet the pending futures counter's value was below maxPendingCount and now reached maxPendingCount.
         * Listener executed in the same thread within method
         * {@link #enqueueUnlimited(CompletableFuture)} or {@link #enqueueBlocking(CompletableFuture)}.
         */
        void onHiLimitReached();

        /**
         * Low limit is calculated based on {@code maxPendingCount * thresholdFactor}.
         * Called, when the pending futures counter's value was above low limit and now reached low limit.
         * Listener executed in separate thread, used by one of Future submitted into {@link PendingFutureLimiter}.
         */
        void onLowLimitSubceed();
    }
}
