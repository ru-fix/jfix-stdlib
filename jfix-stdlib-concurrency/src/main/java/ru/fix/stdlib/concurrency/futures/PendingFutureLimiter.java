package ru.fix.stdlib.concurrency.futures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * maxPendingCount - max amount of pending futures could be registered in limiter instance
 *
 * @author Kamil Asfandiyarov
 */
public class PendingFutureLimiter {
    private final Logger log = LoggerFactory.getLogger(PendingFutureLimiter.class);

    private final AtomicLong counter = new AtomicLong();
    private final ConcurrentHashMap<CompletableFuture, Long> pendingCompletableFutures = new ConcurrentHashMap<>();
    private volatile int maxPendingCount;
    private volatile float thresholdFactor = 0.3f;
    private volatile long maxFutureExecuteTime;
    private volatile long waitTimeToCheckSizeQueue = TimeUnit.MINUTES.toMillis(1);

    private ThresholdListener thresholdListener;

    /**
     * @param maxPendingCount         max amount of {@link CompletableFuture} started in parallel
     * @param maxFutureExecuteTimeout maximum time (milliseconds) on {@link CompletableFuture} execution,
     *                                when this timeout is reached, {@link CompletableFuture} queue will be cleaned,
     *                                and on the {@link CompletableFuture} itself
     *                                {@link CompletableFuture#completeExceptionally(Throwable)} will be invoked
     *
     */
    public PendingFutureLimiter(int maxPendingCount, long maxFutureExecuteTimeout) {
        this.maxPendingCount = maxPendingCount;
        this.maxFutureExecuteTime = maxFutureExecuteTimeout;
    }

    /**
     * @param maxPendingCount         max amount of {@link CompletableFuture} started in parallel
     * @param maxFutureExecuteTimeout maximum time (milliseconds) on {@link CompletableFuture} execution,
     *                                when this timeout is reached, {@link CompletableFuture} queue will be cleaned,
     *                                and on the {@link CompletableFuture} itself
     *                                {@link CompletableFuture#completeExceptionally(Throwable)} will be invoked
     */
    public PendingFutureLimiter(DynamicProperty<Integer> maxPendingCount, DynamicProperty<Long> maxFutureExecuteTimeout) {
        this.maxPendingCount = maxPendingCount.get();
        this.maxFutureExecuteTime = maxFutureExecuteTimeout.get();

        maxPendingCount.addListener((oldValue, newValue) -> setMaxPendingCount(newValue));
        maxFutureExecuteTimeout.addListener((oldValue, newValue) -> setMaxFutureExecuteTime(newValue));
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

    /**
     * Изменяет максимальное число pending futures
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
     * When it is reached the signal about critical workload subceeded will be sent to
     * {@link ThresholdListener#onLowLimitSubceed()}.
     * I. e., to get a to get a threshold 800 with maxPendingCount 1000,
     * should set thresholdFactor = 0.2
     *
     * @param thresholdFactor % of free slots from maxPendingCount,
     *                        when it is reached the signal about critical workload subceeded will be sent
     *                        to {@link ThresholdListener#onLowLimitSubceed()}
     */
    public PendingFutureLimiter changeThresholdFactor(float thresholdFactor) {

        int threashold = calculateThreshold(maxPendingCount, thresholdFactor);

        if (threashold <= 0 || threashold >= maxPendingCount) {
            throw new IllegalArgumentException("Invalid thresholdFactor");
        }

        this.thresholdFactor = thresholdFactor;

        synchronized (counter) {
            counter.notifyAll();
        }

        return this;
    }

    /**
     * Unlimited adding of futures to the queue
     */
    public <T> CompletableFuture<T> enqueueUnlimited(CompletableFuture<T> future) throws InterruptedException {
        return internalEnqueue(future, false);
    }

    /**
     * Limited adding of the future to the queue
     * If the queue has already reached maxPendingCount then block invoking thread till the queue releases,
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

        counter.incrementAndGet();
        pendingCompletableFutures.put(future, System.currentTimeMillis());
        future.handleAsync((any, exc) -> {
            if (exc != null) {
                log.error(exc.getMessage(), exc);
            }
            long value = counter.decrementAndGet();
            pendingCompletableFutures.remove(future);

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
        return future;
    }

    private void awaitOpportunityToEnqueueAndPurge() throws InterruptedException {
        synchronized (counter) {
            while (counter.get() >= maxPendingCount && maxFutureExecuteTime > 0) {
                counter.wait(waitTimeToCheckSizeQueue);
                releaseTimeoutedIfPossible();
            }
        }
    }

    public long getPendingCount() {
        return counter.get();
    }

    /**
     * wait all futures to complete up to timeoutMillis milliseconds
     * If maxFutureExecuteTime is set to 0 it will block invoking thread up to timeoutMillis, till all futures are complete
     * If maxFutureExecuteTime > 0 then it will block invoking thread up to timeoutMillis, till all futures are complete or reach maxFutureExecuteTime
     * @param timeoutMillis
     * maximum time to block invoking thread
     * @return
     * true, if all threads where completed or reached maxFutureExecuteTime in timeoutMillis milliseconds
     * false, if some future(s) remained uncompleted and didn't reach maxFutureExecuteTime in timeoutMillis milliseconds.
     * In this case, uncompleted futures will remain in the queue.
     */
    public boolean waitAll(long timeoutMillis) throws InterruptedException {
        long start = System.currentTimeMillis();
        synchronized (counter) {
            while (counter.get() > 0) {
                releaseTimeoutedIfPossible();
                if (System.currentTimeMillis() - start > timeoutMillis && counter.get() > 0 ) {
                    Throwable t = (new Throwable("Waiting pending futures to complete failed in " + timeoutMillis + " milliseconds. "
                     + counter.get() + " futures remain in the queue."));
                    log.error(t.getMessage(), t);
                    return false;
                }
                if (counter.get() > 0) {
                    counter.wait(waitTimeToCheckSizeQueue);
                }
            }
            return true;
        }
    }

    /**
     * Wait all futures to complete
     * If maxFutureExecuteTime is set to 0 it will block invoking thread till all futures are complete
     * If maxFutureExecuteTime > 0 then it will block invoking thread till all futures are complete or reach maxFutureExecuteTime
     * execution time limit
     */
    public void waitAll() throws InterruptedException {
        synchronized (counter) {
            while (counter.get() > 0) {
                counter.wait(waitTimeToCheckSizeQueue);
            }
            releaseTimeoutedIfPossible();
        }
    }

    private void releaseTimeoutedIfPossible() {
        if (maxFutureExecuteTime == 0) {
            return;
        }

        String errorMessage = "Timeout exception. Completable future did not complete for at least "
                + maxFutureExecuteTime + " milliseconds. Pending count: " + getPendingCount();

        Consumer<Map.Entry<CompletableFuture, Long>> completeExceptionally =
                entry -> entry.getKey().completeExceptionally(new Exception(errorMessage));

        Predicate<Map.Entry<CompletableFuture, Long>> isTimeoutPredicate =
                entry -> System.currentTimeMillis() - entry.getValue() > maxFutureExecuteTime;

        log.error(errorMessage);

        pendingCompletableFutures.entrySet().stream().filter(isTimeoutPredicate).forEach(completeExceptionally);
    }

    public interface ThresholdListener {
        /**
         * Called, whet the inner counter's value reaches maxPendingCount
         * and executed in the same thread withe invoking method.
         */
        void onHiLimitReached();

        /**
         * Called, when the pending futures amount = maxPendingCount * thresholdFactor
         * executed in the separate thread
         */
        void onLowLimitSubceed();
    }
}
