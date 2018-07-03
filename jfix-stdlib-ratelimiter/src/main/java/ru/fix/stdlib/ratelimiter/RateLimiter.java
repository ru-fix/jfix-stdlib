package ru.fix.stdlib.ratelimiter;

import java.time.temporal.TemporalUnit;

public interface RateLimiter extends AutoCloseable {

    /**
     * Get configured rate limit
     */
    double getRate();

    /**
     * Try to acquire permit without waiting for it to become available.
     *
     * @return <code>true</code> if permit has been acquired <code>false</code> otherwise
     */
    boolean tryAcquire();

    /**
     * Try to acquire permit. If it isn't immediately available waits for it specified timeout.
     *
     * @return <code>true</code> if permit has been acquired <code>false</code> otherwise
     */
    boolean tryAcquire(long timeout, TemporalUnit unit);

    /**
     * Update configuration
     */
    void updateRate(int permitsPerSecond);
}
