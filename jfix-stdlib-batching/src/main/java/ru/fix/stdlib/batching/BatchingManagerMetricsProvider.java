package ru.fix.stdlib.batching;

import ru.fix.aggregating.profiler.IndicationProvider;
import ru.fix.aggregating.profiler.Profiler;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

/** This class used in BatchingManagerThread to measure some useful metrics
 *  and designed for single threaded usage only.
 */
public class BatchingManagerMetricsProvider {

    private final String batchManagerId;
    private final Profiler profiler;
    private final String batchProcessorThreadAwaitMetric;
    private final String batchManagerOperationsAwaitMetric;
    private final List<String> operationQueueSizeIndicatorsList = new ArrayList<>();

    BatchingManagerMetricsProvider(String batchManagerId, Profiler profiler) {
        this.batchManagerId = batchManagerId;

        this.batchProcessorThreadAwaitMetric =
                "BatchingManagerThread." + batchManagerId + ".batch_processor_thread_await";
        this.batchManagerOperationsAwaitMetric = "BatchingManagerThread." + batchManagerId + ".operations_await";
        this.profiler = profiler;
    }

    public Boolean profileBatchProcessorAwaitThread(Supplier<Boolean> awaiter) {
        return profiler.profile(this.batchProcessorThreadAwaitMetric, awaiter);
    }

    public Boolean profileBatchManagerThreadAwaitOperation(Supplier<Boolean> awaiter) {
        return profiler.profile(this.batchManagerOperationsAwaitMetric, awaiter);
    }

    public void createOperationsQueueSizeIndicatorIfNeeded(
            String key,
            Queue<?> operations
    ) {
        if (!operationQueueSizeIndicatorsList.contains(key)) {
            String metricName =
                    "BatchingManagerThread." + batchManagerId + "." + key + ".operations.queue.size";

            profiler.attachIndicator(metricName, new OperationsQueueSizeProvider(operations));
            operationQueueSizeIndicatorsList.add(key);
        }
    }

    public void profileTimeOperationSpentInQueue(long creationTimestamp, String key) {
        long spentInQueue = System.currentTimeMillis() - creationTimestamp;
        String metricName =
                "BatchingManagerThread." + batchManagerId + "." + key + ".operation.spent.in.queue";
        profiler.profiledCall(metricName).call(spentInQueue);
    }

    public void detachOperationQueueSizeIndicators() {
        operationQueueSizeIndicatorsList.forEach(profiler::detachIndicator);
        operationQueueSizeIndicatorsList.clear();
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
