package ru.fix.stdlib.ratelimiter;

import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.internal.AtomicRateLimiter;

import java.time.Duration;
import java.time.temporal.TemporalUnit;


public class ConfigurableRateLimiter implements RateLimiter {

    private volatile io.github.resilience4j.ratelimiter.RateLimiter rateLimiter;

    public ConfigurableRateLimiter(String name, int permitsPerSecond) {
        // AtomicRateLimiter does not distribute events inside refresh period.
        // It releases all available permits immediately on interval start if there is a demand for them.
        // To distribute event in our interval of 1 second we divide it
        // into chunks so that 1 chunk of time is limited to 1 permit.
        rateLimiter = new AtomicRateLimiter(
            name,
            RateLimiterConfig.custom()
                .limitForPeriod(1)
                .limitRefreshPeriod(Duration.ofNanos(1_000_000_000 / permitsPerSecond))
                .build()
        );
    }

    @Override
    public boolean tryAcquire() {
        return rateLimiter.getPermission(Duration.ZERO);
    }

    @Override
    public boolean tryAcquire(long timeout, TemporalUnit unit) {
        return rateLimiter.getPermission(Duration.of(timeout, unit));
    }

    @Override
    public double getRate() {
        long refreshPeriodInNanos = rateLimiter.getRateLimiterConfig().getLimitRefreshPeriodInNanos();

        return 1_000_000_000 / refreshPeriodInNanos;
    }

    @Override
    public void updateRate(int permitsPerSecond) {
        String name = rateLimiter.getName();

        // We can update rate limiter at any time because limits for period is always set to 1.
        // So at most we exceed limit by 1
        rateLimiter = new AtomicRateLimiter(
            name,
            RateLimiterConfig.custom()
                .limitForPeriod(1)
                .limitRefreshPeriod(Duration.ofNanos(1_000_000_000 / permitsPerSecond))
                .build()
        );
    }

    @Override
    public void close() {
        // Nothing to do
    }
}
