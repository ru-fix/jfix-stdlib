package ru.fix.stdlib.batching;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Identity;
import ru.fix.aggregating.profiler.ProfiledCall;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.NamedExecutors;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class BatchingManagerThread<ConfigT, PayloadT, KeyT> implements Runnable {

    private static final String THREAD_INTERRUPTED_MESSAGE = "BatchProcessorManager thread interrupted";

    private final Logger log = LoggerFactory.getLogger(BatchingManagerThread.class);

    private final AtomicBoolean isShutdown = new AtomicBoolean();
    private final ExecutorService batchProcessorManagerExecutor = Executors.newSingleThreadExecutor();
    private final ThreadPoolExecutor batchProcessorPool;
    private final BatchingParameters batchingParameters;
    private final Map<KeyT, Queue<Operation<PayloadT>>> pendingTableOperations;

    /**
     * Handle max count of parallel running {@link ru.fix.stdlib.batching.BatchProcessor}
     */
    private final Semaphore batchProcessorsTracker;

    private final Object waitForOperationLock = new Object();
    private final ConfigT config;
    private final BatchTask<ConfigT, PayloadT, KeyT> batchTask;

    private final Profiler profiler;
    private final String batchManagerId;

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

        /*
         * batchThreads for BatchProcessors
         */
        batchProcessorsTracker = new Semaphore(batchingParameters.getBatchThreads());

        this.pendingTableOperations = pendingTableOperations;
        this.batchingParameters = batchingParameters;

        this.config = config;
        this.profiler = profiler;
        this.batchManagerId = batchManagerId;
        this.batchTask = batchTask;
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

        Map<KeyT, String> indicatorMap = new HashMap<>();

        while (!isShutdown.get()) {

            log.trace("Check if thread interrupted");
            if (Thread.currentThread().isInterrupted()) {
                log.trace(THREAD_INTERRUPTED_MESSAGE);
                return;
            }

            log.trace("Wait available thread in BatchProcessor thread pool");
            boolean isBatchProcessorThreadAvailable = false;
            ProfiledCall threadAwaitCall = profiler.start("BatchingManagerThread_batch_processor_thread_await");
            while (!isShutdown.get() && !isBatchProcessorThreadAvailable) {
                try {
                    isBatchProcessorThreadAvailable = batchProcessorsTracker.tryAcquire(100,
                            TimeUnit.MILLISECONDS);
                    if (isBatchProcessorThreadAvailable) {
                        log.trace("isBatchProcessorThreadAvailable");
                        batchProcessorsTracker.release();
                    }
                } catch (InterruptedException exc) {
                    log.trace(THREAD_INTERRUPTED_MESSAGE, exc);
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            threadAwaitCall.stop();

            log.trace("Wait for available operations and copy them to local buffer.");
            List<Operation<PayloadT>> buffer = null;
            KeyT key = null;
            while (buffer == null && !isShutdown.get()) {
                Map.Entry<KeyT, Queue<Operation<PayloadT>>> mappedQueue = pendingTableOperations.entrySet().stream()
                        .map( entry -> checkAndAttachIndicator(indicatorMap, entry))
                        .filter(e -> isBatchFull(e.getValue()) || isTimeoutExpired(e.getValue()))
                        .findAny()
                        .orElse(null);

                if (mappedQueue != null) {
                    key = mappedQueue.getKey();
                    Queue<Operation<PayloadT>> operationsQueue = mappedQueue.getValue();
                    profileTimeSpentInQueueForTable(key, operationsQueue);
                    buffer = Stream.generate(operationsQueue::poll)
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
                } else {
                    log.trace("calculate time to sleep");
                    Optional<Long> oldestCreationTimestamp = pendingTableOperations.values().stream()
                            .map(operations -> Optional.ofNullable(operations.peek()))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .map(Operation::getCreationTimestamp)
                            .min(Long::compareTo);

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
                    ProfiledCall operationsWait = profiler.start("BatchingManagerThread_operations_await");
                    synchronized (waitForOperationLock) {
                        try {
                            if (waitTime <= 0) {
                                waitTime = 1;
                            }
                            waitForOperationLock.wait(waitTime);
                            log.trace("leave wait section");
                        } catch (InterruptedException exc) {
                            log.trace(THREAD_INTERRUPTED_MESSAGE, exc);
                            detachIndicators(indicatorMap);
                            Thread.currentThread().interrupt();
                            return;
                        } finally {
                            operationsWait.stop();
                        }
                    }
                }
            }

            if (!isShutdown.get() && buffer != null) {
                log.trace("Start batch processor");
                batchProcessorPool.execute(new BatchProcessor<>(config,
                        buffer,
                        batchProcessorsTracker,
                        batchTask,
                        key,
                        batchManagerId,
                        profiler));
            }
        }

        detachIndicators(indicatorMap);
        log.trace("BatchProcessorManager thread stopped.");
    }

    private void profileTimeSpentInQueueForTable(KeyT key, Queue<Operation<PayloadT>> operationsQueue) {
        Operation<PayloadT> firstOperation = operationsQueue.peek();
        if (firstOperation != null) {
            long spentInQueue = System.currentTimeMillis() - firstOperation.getCreationTimestamp();
            Map<String, String> tags = new HashMap<>();
            tags.put("table", key.toString());
            Identity identity = new Identity("BatchingManagerThread_operation_spent_in_queue", tags);
            profiler.profiledCall(identity).call(spentInQueue);
        }
    }

    @NotNull
    private Map.Entry<KeyT, Queue<Operation<PayloadT>>> checkAndAttachIndicator(
            Map<KeyT, String> indicatorMap,
            Map.Entry<KeyT, Queue<Operation<PayloadT>>> entry
    ) {
        KeyT mapKey = entry.getKey();
        String indicatorName = "BatchingManagerThread_" + mapKey.toString() + "_operations_queue_size";
        if (!indicatorMap.containsKey(mapKey)) {
            profiler.attachIndicator(
                    indicatorName,
                    () -> (long) pendingTableOperations.get(mapKey).size()
            );
          indicatorMap.put(mapKey, indicatorName);
      }
        return entry;
    }

    private void detachIndicators(Map<KeyT, String> indicatorMap) {
        indicatorMap.values().forEach(profiler::detachIndicator);
        indicatorMap.clear();
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

    public void setThreadCount(int newThreadCount) throws InterruptedException {
        /*
         * guard updating batchProcessorPool size
         */
        synchronized (batchProcessorPool) {

            batchingParameters.setBatchThreads(newThreadCount);

            int difference = newThreadCount - batchProcessorPool.getCorePoolSize();

            if (difference > 0) {
                /*
                 * increase pool size to abs(difference)
                 */
                batchProcessorPool.setCorePoolSize(newThreadCount);
                batchProcessorPool.setMaximumPoolSize(newThreadCount);
                batchProcessorsTracker.release(difference);

            } else if (difference < 0) {
                /*
                 * decrease pool size to abs(difference)
                 */
                batchProcessorsTracker.acquire(-difference);
                batchProcessorPool.setCorePoolSize(newThreadCount);
                batchProcessorPool.setMaximumPoolSize(newThreadCount);
            }
        }
    }
}
