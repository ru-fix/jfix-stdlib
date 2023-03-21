package ru.fix.stdlib.batching;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.lang.String.format;

/**
 * ConfigT - instance of ConfigT will be passed to {@link  ru.fix.stdlib.batching.BatchTask}
 * PayloadT - data that will be collect in batches and passed to {@link  ru.fix.stdlib.batching.BatchTask}
 * KeyT - {@link BatchingManager} will create separate batch queue for each key.
 *
 * @author Kamil Asfandiyarov
 */
public class BatchingManager<ConfigT, PayloadT, KeyT> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BatchingManager.class);

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
    private final BatchingManagerThread<ConfigT, PayloadT, KeyT> batchingManagerThread;

    private final String batchManagerId;

    private final Profiler profiler;

    public BatchingManager(ConfigT config,
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

        this.batchingManagerThread = new BatchingManagerThread<>(config,
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
     * @throws MaxPendingOperationExceededException see {@link #setMaxPendingOperations(int)}
     * @throws InterruptedException                 if thread was interrupted while waiting in blocking mode
     */
    public void enqueue(KeyT key, PayloadT payload) throws MaxPendingOperationExceededException, InterruptedException {
        log.trace("enqueue(key: {}, payload: {}", key, payload);

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
        if (log.isTraceEnabled()) {
            log.trace("Queue size for key {} is {}", key, operationQueue.size());
        }
    }

    private boolean isBatchLimitReached(Queue<Operation<PayloadT>> operationQueue) {
        return operationQueue.size() >= batchingParameters.getMaxPendingOperations();
    }

    /**
     * @deprecated Number of threads should be managed via {@link BatchingParameters#getPoolSettings()}
     * and property subscription.
     * @param batchThreads How many batch operations will be performed in parallel.
     * @return current BatchingManager
     */
    @Deprecated
    public BatchingManager<ConfigT, PayloadT, KeyT> setBatchThreads(int batchThreads) {
        batchingManagerThread.setThreadCount(batchThreads);
        return this;
    }

    public BatchingManager<ConfigT, PayloadT, KeyT> setBatchSize(int batchSize) {
        batchingParameters.setBatchSize(batchSize);
        return this;
    }

    public BatchingManager<ConfigT, PayloadT, KeyT> setMaxPendingOperations(int maxPendingOperations) {
        batchingParameters.setMaxPendingOperations(maxPendingOperations);
        return this;
    }

    public BatchingManager<ConfigT, PayloadT, KeyT> setBatchTimeout(int batchTimeout) {
        batchingParameters.setBatchTimeout(batchTimeout);
        return this;
    }

}
