package ru.fix.stdlib.batching;

import org.slf4j.Logger;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.BooleanSupplier;

class BatchProcessorsTracker {

    private static final Duration DEFAULT_TIMEOUT = Duration.of(100, ChronoUnit.MILLIS);

    private final Object waitForBatchProcessorPoolLock = new Object();
    private final Logger log;

    BatchProcessorsTracker(Logger log) {
        this.log = log;
    }

    boolean isBatchProcessorThreadAvailable(BooleanSupplier condition) throws InterruptedException {
        return isBatchProcessorThreadAvailable(DEFAULT_TIMEOUT, condition);
    }

    boolean isBatchProcessorThreadAvailable(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        boolean isBatchProcessorThreadAvailable = condition.getAsBoolean();

        if (isBatchProcessorThreadAvailable) {
            log.trace("isBatchProcessorThreadAvailable");
        } else {
            synchronized (waitForBatchProcessorPoolLock) {
                waitForBatchProcessorPoolLock.wait(timeout.toMillis());
            }
        }

        return isBatchProcessorThreadAvailable;
    }

    void notifyAboutAvailableBatchProcessorThread() {
        synchronized (waitForBatchProcessorPoolLock) {
            waitForBatchProcessorPoolLock.notifyAll();
        }
    }
}
