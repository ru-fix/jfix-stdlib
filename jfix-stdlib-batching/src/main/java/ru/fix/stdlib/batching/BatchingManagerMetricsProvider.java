package ru.fix.stdlib.batching;

import ru.fix.aggregating.profiler.Identity;
import ru.fix.aggregating.profiler.IndicationProvider;
import ru.fix.aggregating.profiler.Profiler;

import java.util.HashSet;
import java.util.Queue;
import java.util.function.Supplier;

/**
 * This class used in BatchingManagerThread to measure some useful metrics
 * and designed for single threaded usage only.
 */
public class BatchingManagerMetricsProvider {

    private final String batchManagerId;
    private final Profiler profiler;
    private final Identity batchProcessorThreadAwaitIdentity;
    private final Identity batchManagerOperationsAwaitIdentity;
    private final HashSet<String> operationQueueSizeIndicatorsList = new HashSet<>();

    public static final String BATCHING_MANAGER_ID_TAG_NAME = "batchingManagerId";
    public static final String BATCHING_MANAGER_KEY_TAG_NAME = "key";

    BatchingManagerMetricsProvider(String batchManagerId, Profiler profiler) {
        this.batchManagerId = batchManagerId;

        this.batchProcessorThreadAwaitIdentity = new Identity(
                "BatchingManagerThread.batch.processor.thread.await",
                BATCHING_MANAGER_ID_TAG_NAME, batchManagerId
        );

        this.batchManagerOperationsAwaitIdentity = new Identity(
                "BatchingManagerThread.operations.await",
                BATCHING_MANAGER_ID_TAG_NAME, batchManagerId
        );
        this.profiler = profiler;
    }

    public Boolean profileBatchProcessorAwaitThread(Supplier<Boolean> awaiter) {
        return profiler.profiledCall(batchProcessorThreadAwaitIdentity).profile(awaiter);
    }

    public Boolean profileBatchManagerThreadAwaitOperation(Supplier<Boolean> awaiter) {
        return profiler.profiledCall(batchManagerOperationsAwaitIdentity).profile(awaiter);
    }

    public void createOperationsQueueSizeIndicatorIfNeeded(
            String key,
            Queue<?> operations
    ) {
        if (!operationQueueSizeIndicatorsList.contains(key)) {
            Identity indicatorIdentity =
                    buildIdentityWithKey("BatchingManager.operations.queue.size", key);
            profiler.attachIndicator(indicatorIdentity, new OperationsQueueSizeProvider(operations));
            operationQueueSizeIndicatorsList.add(key);
        }
    }

    public void profileTimeOperationSpentInQueue(long creationTimestamp, String key) {
        Identity identity =
                buildIdentityWithKey("BatchingManager.operation.spent.in.queue", key);
        profiler.profiledCall(identity).call(creationTimestamp);
    }

    public void detachOperationQueueSizeIndicators() {
        operationQueueSizeIndicatorsList.forEach(profiler::detachIndicator);
        operationQueueSizeIndicatorsList.clear();
    }

    private Identity buildIdentityWithKey(String name, String key) {
        return new Identity(
                name,
                BATCHING_MANAGER_ID_TAG_NAME, batchManagerId,
                BATCHING_MANAGER_KEY_TAG_NAME, key
        );
    }

    private static class OperationsQueueSizeProvider implements IndicationProvider {

        private final Queue<?> operations;

        OperationsQueueSizeProvider(Queue<?> operations) {
            this.operations = operations;
        }

        @Override
        public Long get() {
            return (long) operations.size();
        }
    }

}
