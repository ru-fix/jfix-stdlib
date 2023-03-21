package ru.fix.stdlib.batching;

import org.jetbrains.annotations.Nullable;
import ru.fix.stdlib.concurrency.settings.ProfiledThreadPoolSettings;

public class BatchingParameters {
    /**
     * Size of batch for put method for each table
     */
    private volatile int batchSize = 200;

    /**
     * Max row in query on sending, over operations per Table
     */
    private volatile int maxPendingOperations = 1000_000;

    /**
     * How long we wait until batch will be full
     * Time in milliseconds
     */
    private volatile int batchTimeout = 10;

    /**
     * How many batch operations will be performed in parallel.
     * @deprecated <b>should be removed in future releases</b>
     */
    @Deprecated
    private volatile int batchThreads = 20;

    /**
     * Block if batch is full and new operation have come
     */
    private volatile boolean blockIfLimitExceeded;

    /**
     * How many batch operations will be performed in parallel.
     * <p>
     * These settings are replacement for {@link #batchThreads}.
     * <p>
     * If {@code poolSettings == null} then {@link #batchThreads} will be used.
     *
     * @see ProfiledThreadPoolSettings
     */
    @Nullable
    private ProfiledThreadPoolSettings poolSettings;

    /**
     * How many batch operations will be performed in parallel.
     */
    public int getBatchThreads() {
        return batchThreads;
    }

    /**
     * How many batch operations will be performed in parallel.
     */
    public BatchingParameters setBatchThreads(int batchThreads) {
        this.batchThreads = batchThreads;
        return this;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public BatchingParameters setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public int getMaxPendingOperations() {
        return maxPendingOperations;
    }

    public BatchingParameters setMaxPendingOperations(int maxPendingOperations) {
        this.maxPendingOperations = maxPendingOperations;
        return this;
    }

    public int getBatchTimeout() {
        return batchTimeout;
    }

    public BatchingParameters setBatchTimeout(int batchTimeout) {
        this.batchTimeout = batchTimeout;
        return this;
    }

    public boolean isBlockIfLimitExceeded() {
        return blockIfLimitExceeded;
    }

    public BatchingParameters setBlockIfLimitExceeded(boolean blockIfLimitExceeded) {
        this.blockIfLimitExceeded = blockIfLimitExceeded;
        return this;
    }

    public ProfiledThreadPoolSettings getPoolSettings() {
        return poolSettings;
    }

    public BatchingParameters setPoolSettings(ProfiledThreadPoolSettings poolSettings) {
        this.poolSettings = poolSettings;
        return this;
    }
}
