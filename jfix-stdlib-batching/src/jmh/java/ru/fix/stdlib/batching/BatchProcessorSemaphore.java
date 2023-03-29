package ru.fix.stdlib.batching;

import ru.fix.aggregating.profiler.Identity;
import ru.fix.aggregating.profiler.ProfiledCall;
import ru.fix.aggregating.profiler.Profiler;

import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import static ru.fix.stdlib.batching.BatchingManagerMetricsProvider.BATCHING_MANAGER_ID_TAG_NAME;
import static ru.fix.stdlib.batching.BatchingManagerMetricsProvider.BATCHING_MANAGER_KEY_TAG_NAME;

/**
 * <b>For usage in jmh tests only!</b>
 * @see BatchingManagerHighContentionJmh
 * @param <ConfigT>
 * @param <PayloadT>
 * @param <KeyT>
 */
class BatchProcessorSemaphore<ConfigT, PayloadT, KeyT> implements Runnable {

    private final ConfigT config;

    private final Semaphore batchProcessorsTracker;
    private final List<Operation<PayloadT>> batch;
    private final BatchTask<ConfigT, PayloadT, KeyT> batchTask;
    private final KeyT key;

    private final Profiler profiler;

    private final ProfiledCall awaitExecution;
    private final Identity handleMetricIdentity;

    BatchProcessorSemaphore(ConfigT config,
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
        this.profiler = profiler;

        handleMetricIdentity = new Identity(
                "Batch.processor.hndl",
                BATCHING_MANAGER_ID_TAG_NAME, batchManagerId,
                BATCHING_MANAGER_KEY_TAG_NAME, key.toString()
        );

        awaitExecution = profiler.profiledCall(new Identity(
                "Batch.processor.await",
                BATCHING_MANAGER_ID_TAG_NAME, batchManagerId,
                BATCHING_MANAGER_KEY_TAG_NAME, key.toString()
        )).start();
    }

    /**
     * Process batch of operations
     */
    @Override
    public void run() {
        try {
            batchProcessorsTracker.acquire();
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
        } finally {
            awaitExecution.stop();
        }

        try {
            List<PayloadT> payloadBatch = batch.stream().map(Operation::getPayload).collect(Collectors.toList());
            try (ProfiledCall profiledCall = profiler.profiledCall(handleMetricIdentity).start()) {
                batchTask.process(config, payloadBatch, key);
                profiledCall.stop(payloadBatch.size());
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
        } finally {
            batchProcessorsTracker.release();
        }
    }
}
