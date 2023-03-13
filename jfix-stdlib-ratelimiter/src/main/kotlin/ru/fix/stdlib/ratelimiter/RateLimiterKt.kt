package ru.fix.stdlib.ratelimiter

import java.time.temporal.TemporalUnit

interface RateLimiterKt : AutoCloseable {

    /**
     * Get configured rate limit
     */
    fun getRate(): Double

    /**
     * Try to acquire permit without waiting for it to become available.
     *
     * @return `true` if permit has been acquired `false` otherwise
     */
    suspend fun tryAcquire(): Boolean

    /**
     * Try to acquire permit. If it isn't immediately available waits for it specified timeout.
     *
     * @return `true` if permit has been acquired `false` otherwise
     */
    suspend fun tryAcquire(timeout: Long, unit: TemporalUnit): Boolean

    /**
     * Update configuration
     */
    fun updateRate(permitsPerSecond: Int)

}