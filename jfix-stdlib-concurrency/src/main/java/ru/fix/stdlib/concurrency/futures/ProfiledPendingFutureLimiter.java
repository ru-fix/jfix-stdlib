package ru.fix.stdlib.concurrency.futures;

import ru.fix.aggregating.profiler.Identity;
import ru.fix.aggregating.profiler.Profiler;
import ru.fix.dynamic.property.api.DynamicProperty;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @author Tim Urmancheev
 */
public class ProfiledPendingFutureLimiter extends PendingFutureLimiter implements AutoCloseable {

    private final Profiler profiler;
    private final Map<String, String> tags;

    public ProfiledPendingFutureLimiter(DynamicProperty<Integer> maxPendingCount, Profiler namedProfiler) {
        super(maxPendingCount.get());
        this.profiler = namedProfiler;
        this.tags = Collections.emptyMap();

        maxPendingCount.addListener((oldValue, newValue) -> setMaxPendingCount(newValue));
        attachIndicators();
    }

    public ProfiledPendingFutureLimiter(DynamicProperty<Integer> maxPendingCount,
                                        DynamicProperty<Long> maxFutureExecuteTimeout,
                                        Profiler namedProfiler) {
        this(maxPendingCount, maxFutureExecuteTimeout, namedProfiler, Collections.emptyMap());
    }

    public ProfiledPendingFutureLimiter(DynamicProperty<Integer> maxPendingCount,
                                        DynamicProperty<Long> maxFutureExecuteTimeout,
                                        Profiler namedProfiler,
                                        Map<String, String> tags) {
        super(maxPendingCount.get(), maxFutureExecuteTimeout.get());
        this.profiler = namedProfiler;
        this.tags = tags;

        maxPendingCount.addListener((oldValue, newValue) -> setMaxPendingCount(newValue));
        maxFutureExecuteTimeout.addListener((oldValue, newValue) -> setMaxFutureExecuteTime(newValue));

        attachIndicators();
    }

    @Override
    protected <T> CompletableFuture<T> internalEnqueue(CompletableFuture<T> future, boolean needToBlock) throws InterruptedException {
        profiler.profileFuture("future_lifetime", () -> future);
        return super.internalEnqueue(future, needToBlock);
    }

    private void attachIndicators() {
        profiler.attachIndicator(new Identity(".pending", tags), this::getPendingCount);
        profiler.attachIndicator(new Identity(".threshold", tags), () -> (long) this.getThreshold());
        profiler.attachIndicator(new Identity(".max_capacity", tags), () -> (long) this.getMaxPendingCount());
    }

    @Override
    public void close() {
        profiler.detachIndicator(new Identity(".pending", tags));
        profiler.detachIndicator(new Identity(".threshold", tags));
        profiler.detachIndicator(new Identity(".max_capacity", tags));
    }
}
