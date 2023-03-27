package ru.fix.stdlib.batching;

import ru.fix.aggregating.profiler.Profiler;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.String.format;

/**
 * <b>For usage in jmh tests only!</b>
 * @see BatchingManagerHighContentionJmh
 * @param <ConfigT>
 * @param <PayloadT>
 * @param <KeyT>
 */
class BatchingManagerSemaphore<ConfigT, PayloadT, KeyT> implements AutoCloseable {

    /**
     * Queues with operation, is limited by maxPendingOperations
     */
    private final Map<KeyT, Queue<Operation<PayloadT>>> pendingTableOperations = new ConcurrentHashMap<>();

    /**
     * Parameters for BatchProcessor
     */
    private final BatchingParameters batchingParameters;

    /**
     * Monitoring queue and create BatchProcessor for executed Row
     */
    private final BatchingManagerThreadSemaphore<ConfigT, PayloadT, KeyT> batchingManagerThread;

    private final String batchManagerId;

    private final Profiler profiler;

    BatchingManagerSemaphore(ConfigT config,
                           BatchTask<ConfigT, PayloadT, KeyT> batchTask,
                           BatchingParameters batchingParameters,
                           String batchManagerId,
                           Profiler profiler) {
        this.batchingParameters = batchingParameters;
        this.batchManagerId = batchManagerId;
        this.profiler = profiler;

        profiler.attachIndicator(getIndicatorName(batchManagerId),
                () -> pendingTableOperations.values().stream().mapToLong(Collection::size).sum()
        );

        this.batchingManagerThread = new BatchingManagerThreadSemaphore<>(config,
                pendingTableOperations,
                batchingParameters,
                profiler,
                batchManagerId,
                batchTask);
        this.batchingManagerThread.start();
    }

    private static String getIndicatorName(String batchManagerId) {
        return "Batching.manager." + batchManagerId;
    }

    @Override
    public void close() {
        batchingManagerThread.shutdown();
        profiler.detachIndicator(getIndicatorName(batchManagerId));
    }

    /**
     * @param key     buffer key. {@link BatchingManager} will create separate batch queue for each key.
     * @param payload data that will be collect in batches and passed to {@link  ru.fix.stdlib.batching.BatchTask}
     * @throws MaxPendingOperationExceededException see {@link BatchingParameters#setMaxPendingOperations(int)}
     * @throws InterruptedException                 if thread was interrupted while waiting in blocking mode
     */
    void enqueue(KeyT key, PayloadT payload) throws MaxPendingOperationExceededException, InterruptedException {
        Queue<Operation<PayloadT>> operationQueue =
                pendingTableOperations.computeIfAbsent(key, k -> new ConcurrentLinkedQueue<>());

        Operation<PayloadT> operation = new Operation<>(payload, System.currentTimeMillis());
        if (isBatchLimitReached(operationQueue)) {
            if (batchingParameters.isBlockIfLimitExceeded()) {
                while (isBatchLimitReached(operationQueue)) {
                    synchronized (operationQueue) {
                        operationQueue.wait();
                    }
                }
            } else {
                String message = format("Queue operations peaked, maxPendingOperations = %d",
                        batchingParameters.getMaxPendingOperations());
                throw new MaxPendingOperationExceededException(message);
            }
        }

        operationQueue.add(operation);
    }

    private boolean isBatchLimitReached(Queue<Operation<PayloadT>> operationQueue) {
        return operationQueue.size() >= batchingParameters.getMaxPendingOperations();
    }
}
