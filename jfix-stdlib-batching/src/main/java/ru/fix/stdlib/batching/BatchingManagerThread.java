package ru.fix.stdlib.batching;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.NamedExecutors;
import ru.fix.stdlib.concurrency.threads.ProfiledThreadPoolExecutor;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class BatchingManagerThread<ConfigT, PayloadT, KeyT> implements Runnable {

    private static final String THREAD_INTERRUPTED_MESSAGE = "BatchProcessorManager thread interrupted";

    private final Logger log = LoggerFactory.getLogger(BatchingManagerThread.class);

    private final AtomicBoolean isShutdown = new AtomicBoolean();
    private final ExecutorService batchProcessorManagerExecutor;
    private final ProfiledThreadPoolExecutor batchProcessorPool;
    private final BatchingParameters batchingParameters;
    private final Map<KeyT, Queue<Operation<PayloadT>>> pendingTableOperations;

    /**
     * Handle max count of parallel running {@link ru.fix.stdlib.batching.BatchProcessor}
     */
    private final BatchProcessorsTracker batchProcessorsTracker = new BatchProcessorsTracker(log);

    private final Object waitForOperationLock = new Object();
    private final ConfigT config;
    private final BatchTask<ConfigT, PayloadT, KeyT> batchTask;

    private final Profiler profiler;
    private final String batchManagerId;
    private final BatchingManagerMetricsProvider metricsProvider;

    public BatchingManagerThread(
            ConfigT config,
            Map<KeyT, Queue<Operation<PayloadT>>> pendingTableOperations,
            BatchingParameters batchingParameters,
            Profiler profiler,
            String batchManagerId, BatchTask<ConfigT, PayloadT, KeyT> batchTask) {


        batchProcessorPool = NamedExecutors.newDynamicPool(
                "batching-manager-" + batchManagerId,
                DynamicProperty.of(batchingParameters.getBatchThreads()),
                profiler
        );

        this.pendingTableOperations = pendingTableOperations;
        this.batchingParameters = batchingParameters;

        this.config = config;
        this.profiler = profiler;
        this.batchManagerId = batchManagerId;
        this.batchTask = batchTask;

        this.batchProcessorManagerExecutor = NamedExecutors.newSingleThreadPool(
                "BatchingManagerThread_ " + batchManagerId, profiler
        );

        metricsProvider = new BatchingManagerMetricsProvider(batchManagerId, profiler);
    }

    private static void shutdownAndAwaitTermination(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
                executor.shutdownNow();
                executor.awaitTermination(1, TimeUnit.SECONDS);
            }
        } catch (InterruptedException ie) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * blocking method will return when all threads stops
     */
    public void shutdown() {
        if (!isShutdown.compareAndSet(false, true)) {
            log.debug("already shutdown");
        }
        shutdownAndAwaitTermination(batchProcessorManagerExecutor);
        shutdownAndAwaitTermination(batchProcessorPool);
    }

    public void start() {
        batchProcessorManagerExecutor.execute(this);
        log.trace("BatchProcessorManagerThread executor started");
    }

    @Override
    public void run() {
        log.trace("BatchProcessorManager thread started.");

        while (!isShutdown.get()) {

            Boolean threadInterrupted;

            log.trace("Check if thread interrupted");
            if (Thread.currentThread().isInterrupted()) {
                log.trace(THREAD_INTERRUPTED_MESSAGE);
                return;
            }

            threadInterrupted = metricsProvider.profileBatchProcessorAwaitThread(
                    this::waitForAvailableBatchProcessThread
            );
            if (threadInterrupted) return;

            log.trace("Wait for available operations and copy them to local buffer.");
            List<Operation<PayloadT>> buffer = null;
            KeyT key = null;
            while (buffer == null && !isShutdown.get()) {

                Map.Entry<KeyT, Queue<Operation<PayloadT>>> mappedQueue = takeAnyQueueReadyForProcessing();

                if (mappedQueue != null) {
                    key = mappedQueue.getKey();
                    buffer = prepareBuffer(mappedQueue);
                } else {
                    threadInterrupted = metricsProvider.profileBatchManagerThreadAwaitOperation(
                            this::calculateTimeForSleepAndWaitForOperations
                    );

                    if (threadInterrupted) return;
                }
            }

            if (!isShutdown.get() && buffer != null) {
                log.trace("Start batch processor");
                executeBatchForBuffer(buffer, key);
            }
        }

        metricsProvider.detachOperationQueueSizeIndicators();
        log.trace("BatchProcessorManager thread stopped.");
    }

    private void executeBatchForBuffer(List<Operation<PayloadT>> buffer, KeyT key) {
        batchProcessorPool.execute(
                new BatchProcessor<>(
                        config,
                        buffer,
                        batchProcessorsTracker,
                        batchTask,
                        key,
                        batchManagerId,
                        profiler
                )
        );
    }

    private boolean waitForAvailableBatchProcessThread() {
        log.trace("Wait available thread in BatchProcessor thread pool");
        boolean isBatchProcessorThreadAvailable = false;
        while (!isShutdown.get() && !isBatchProcessorThreadAvailable) {
            try {
                isBatchProcessorThreadAvailable = batchProcessorsTracker.isBatchProcessorThreadAvailable(() ->
                        batchProcessorPool.getQueue().size() < batchProcessorPool.getMaximumPoolSize()
                );
            } catch (InterruptedException exc) {
                log.trace(THREAD_INTERRUPTED_MESSAGE, exc);
                Thread.currentThread().interrupt();
                return true;
            }
        }
        return false;
    }

    @Nullable
    private Map.Entry<KeyT, Queue<Operation<PayloadT>>> takeAnyQueueReadyForProcessing() {
        return pendingTableOperations.entrySet().stream()
                .peek(entry ->
                        metricsProvider.createOperationsQueueSizeIndicatorIfNeeded(
                                entry.getKey().toString(), entry.getValue()
                        )
                ).filter(e -> isBatchFull(e.getValue()) || isTimeoutExpired(e.getValue()))
                .findAny()
                .orElse(null);
    }

    private boolean calculateTimeForSleepAndWaitForOperations() {
        log.trace("calculate time to sleep");
        Optional<Long> oldestCreationTimestamp = getOldestOperationCreationTimeStamp();

        long waitTime;
        if (oldestCreationTimestamp.isPresent()) {
            waitTime = batchingParameters.getBatchTimeout() - (System.currentTimeMillis() -
                    oldestCreationTimestamp.get());
            if (waitTime < 0) {
                waitTime = 1;
            }
        } else {
            waitTime = batchingParameters.getBatchTimeout();
        }
        log.trace("queue is empty, wait {}ms", waitTime);
        synchronized (waitForOperationLock) {
            try {
                if (waitTime <= 0) {
                    waitTime = 1;
                }
                waitForOperationLock.wait(waitTime);
                log.trace("leave wait section");
            } catch (InterruptedException exc) {
                log.trace(THREAD_INTERRUPTED_MESSAGE, exc);
                metricsProvider.detachOperationQueueSizeIndicators();
                Thread.currentThread().interrupt();
                return true;
            }
        }
        return false;
    }

    @NotNull
    private Optional<Long> getOldestOperationCreationTimeStamp() {
        return pendingTableOperations.values().stream()
                .map(operations -> Optional.ofNullable(operations.peek()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(Operation::getCreationTimestamp)
                .min(Long::compareTo);
    }

    private List<Operation<PayloadT>> prepareBuffer(Map.Entry<KeyT, Queue<Operation<PayloadT>>> queueEntry) {

        KeyT key = queueEntry.getKey();
        Queue<Operation<PayloadT>> operationsQueue = queueEntry.getValue();
        profileTimeSpentInQueueForTable(key, operationsQueue);
        List<Operation<PayloadT>> buffer = Stream.generate(operationsQueue::poll)
                .limit(getBatchSize())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (batchingParameters.isBlockIfLimitExceeded()) {
            synchronized (operationsQueue) {
                operationsQueue.notifyAll();
            }
        }
        log.trace("Prepared buffer for \"{}\" table, buffer size {}, queue size {}", key, buffer.size(),
                operationsQueue.size());
        return buffer;
    }

    private void profileTimeSpentInQueueForTable(KeyT key, Queue<Operation<PayloadT>> operationsQueue) {
        Operation<PayloadT> firstOperation = operationsQueue.peek();
        if (firstOperation != null) {
            metricsProvider.profileTimeOperationSpentInQueue(firstOperation.getCreationTimestamp(), key.toString());
        }
    }

    private int getBatchSize() {
        return batchingParameters.getBatchSize();
    }


    private boolean isBatchFull(Queue<Operation<PayloadT>> buffer) {
        return buffer.size() >= batchingParameters.getBatchSize();
    }

    private boolean isTimeoutExpired(Queue<Operation<PayloadT>> queue) {
        Operation<PayloadT> operation = queue.peek();
        if (operation != null) {
            long howMuchTimeHasPassed = System.currentTimeMillis() - operation.getCreationTimestamp();
            return howMuchTimeHasPassed > batchingParameters.getBatchTimeout();
        } else {
            return false;
        }
    }

    public void setThreadCount(int newThreadCount) {
        /*
         * guard updating batchProcessorPool size
         */
        synchronized (batchProcessorPool) {
            batchingParameters.setBatchThreads(newThreadCount);
            batchProcessorPool.setMaxPoolSize(newThreadCount);
        }
    }
}
