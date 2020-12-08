package ru.fix.stdlib.batching;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;
import ru.fix.stdlib.concurrency.threads.NamedExecutors;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
                "batching-manager",
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
        while (!isShutdown.get()) {

            log.trace("Check if thread interrupted");
            if (Thread.currentThread().isInterrupted()) {
                log.trace(THREAD_INTERRUPTED_MESSAGE);
                return;
            }

            log.trace("Wait available thread in BatchProcessor thread pool");
            boolean isBatchProcessorThreadAvailable = false;
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

            log.trace("Wait for available operations and copy them to local buffer.");
            List<Operation<PayloadT>> buffer = null;
            KeyT key = null;
            while (buffer == null && !isShutdown.get()) {
                Map.Entry<KeyT, Queue<Operation<PayloadT>>> mappedQueue = pendingTableOperations.entrySet().stream()
                        .filter(e -> isBathFull(e.getValue()) || isTimeoutExpired(e.getValue()))
                        .findAny()
                        .orElse(null);

                if (mappedQueue != null) {
                    key = mappedQueue.getKey();
                    Queue<Operation<PayloadT>> operationsQueue = mappedQueue.getValue();
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
                    Optional<Long> oldestCreationTimestatmp = pendingTableOperations.values().stream()
                            .map(operations -> Optional.ofNullable(operations.peek()))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .map(Operation::getCreationTimestamp)
                            .min(Long::compareTo);

                    long waitTime;
                    if (oldestCreationTimestatmp.isPresent()) {
                        waitTime = batchingParameters.getBatchTimeout() - (System.currentTimeMillis() -
                                oldestCreationTimestatmp.get());
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
                            Thread.currentThread().interrupt();
                            return;
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
        log.trace("BatchProcessorManager thread stopped.");
    }

    private int getBatchSize() {
        return batchingParameters.getBatchSize();
    }


    private boolean isBathFull(Queue<Operation<PayloadT>> buffer) {
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
