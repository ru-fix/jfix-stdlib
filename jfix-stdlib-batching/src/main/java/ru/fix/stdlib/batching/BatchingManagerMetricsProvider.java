package ru.fix.stdlib.batching;

import org.jetbrains.annotations.NotNull;
import ru.fix.aggregating.profiler.IndicationProvider;
import ru.fix.aggregating.profiler.ProfiledCall;
import ru.fix.aggregating.profiler.Profiler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class BatchingManagerMetricsProvider {

    private final String batchManagerId;
    private final Profiler profiler;
    private final String batchProcessorThreadAwaitMetric;
    private final String batchManagerOperationsAwaitMetric;
    private final List<String> operationQueueSizeIndicatorsList = new ArrayList<>();
    private ProfiledCall batchProcessorThreadAwaitProfiledCall;
    private ProfiledCall batchingManagerOperationsAwaitProfiledCall;

    BatchingManagerMetricsProvider(String batchManagerId, Profiler profiler) {
        this.batchManagerId = batchManagerId;

        this.batchProcessorThreadAwaitMetric =
                "BatchingManagerThread." + batchManagerId + ".batch_processor_thread_await";
        this.batchManagerOperationsAwaitMetric = "BatchingManagerThread." + batchManagerId + ".operations_await";
        this.profiler = profiler;
    }


    public void startBatchProcessorThreadAwaitProfiling() {
        this.batchProcessorThreadAwaitProfiledCall = profiler.start(batchProcessorThreadAwaitMetric);
    }

    public void stopBatchManagerThreadAwaitProfiling() {
        this.batchProcessorThreadAwaitProfiledCall.stop();
    }

    public void startBatchingManagerAwaitOperationsProfiling() {
        this.batchingManagerOperationsAwaitProfiledCall = profiler.start(batchManagerOperationsAwaitMetric);
    }

    public void stopBatchingManagerAwaitOperationsProfiling() {
        this.batchingManagerOperationsAwaitProfiledCall.stop();
    }

    @NotNull
    public <KeyT, PayloadT> Map.Entry<KeyT, Queue<Operation<PayloadT>>> checkAndAddOperationsQueueSizeForTableIndicator(
            Map.Entry<KeyT, Queue<Operation<PayloadT>>> entry
    ) {
        KeyT mapKey = entry.getKey();
        if (!operationQueueSizeIndicatorsList.contains(mapKey.toString())) {
            String metricName =
                    "BatchingManagerThread." + batchManagerId + "." + mapKey + ".operations.queue.size";

            profiler.attachIndicator(metricName, new OperationsQueueSizeProvider<>(entry.getValue()));
            operationQueueSizeIndicatorsList.add(mapKey.toString());
        }
        return entry;
    }

    public void detachOperationQueueSizeIndicators() {
        operationQueueSizeIndicatorsList.forEach(profiler::detachIndicator);
        operationQueueSizeIndicatorsList.clear();
    }

    private static class OperationsQueueSizeProvider<PayloadT> implements IndicationProvider {

        private final Queue<Operation<PayloadT>> operations;

        OperationsQueueSizeProvider(Queue<Operation<PayloadT>> operations) {
            this.operations = operations;
        }

        @Override
        public Long get() {
            return (long) operations.size();
        }
    }

}
