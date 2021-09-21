package ru.fix.stdlib.batching;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.ProfiledCall;
import ru.fix.aggregating.profiler.Profiler;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

class BatchProcessor<ConfigT, PayloadT, KeyT> implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(BatchProcessor.class);

    private final ConfigT config;

    private final Semaphore batchProcessorsTracker;
    private final List<Operation<PayloadT>> batch;
    private final BatchTask<ConfigT, PayloadT, KeyT> batchTask;
    private final KeyT key;

    private final Profiler profiler;
    private final String batchManagerId;

    private final ProfiledCall awaitExecution;

    public BatchProcessor(ConfigT config,
                          List<Operation<PayloadT>> batch,
                          Semaphore batchProcessorsTracker,
                          BatchTask<ConfigT, PayloadT, KeyT> batchTask, KeyT key,
                          String batchManagerId,
                          Profiler profiler) {
        this.batchProcessorsTracker = batchProcessorsTracker;
        this.config = config;
        this.batch = batch;
        this.batchTask = batchTask;
        this.key = key;
        this.batchManagerId = batchManagerId;
        this.profiler = profiler;

        awaitExecution = profiler.start(getMetricName("await"));
    }

    /**
     * Process batch of operations
     */
    @Override
    public void run() {
        try {
            log.trace("acquire semaphore");
            batchProcessorsTracker.acquire();
        } catch (InterruptedException exc) {
            log.error("BatchProcessor thread interrupted", exc);
            Thread.currentThread().interrupt();
        } finally {
            awaitExecution.stop();
        }

        try {
            List<PayloadT> payloadBatch = batch.stream().map(Operation::getPayload).collect(Collectors.toList());
            try (ProfiledCall profiledCall = profiler.start(getMetricName("hndl"))) {
                batchTask.process(config, payloadBatch, key);
                profiledCall.stop(payloadBatch.size());
            }
        } catch (InterruptedException interruptedException) {
            log.error("BatchProcessor thread was interrupted.", interruptedException);
            Thread.currentThread().interrupt();
        } catch (Exception exc) {
            log.error("Exception during preparing and sending batch", exc);
        } finally {
            log.trace("release semaphore");
            batchProcessorsTracker.release();
        }
    }

    private String getMetricName(String suffix) {
        return "Batch.processor." + suffix + "." + batchManagerId + "." + key;
    }
}
