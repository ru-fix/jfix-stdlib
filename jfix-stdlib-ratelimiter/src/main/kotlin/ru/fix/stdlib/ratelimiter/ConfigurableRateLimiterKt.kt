package ru.fix.stdlib.ratelimiter

import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.github.resilience4j.ratelimiter.internal.AtomicRateLimiter
import kotlinx.coroutines.delay
import java.time.Duration
import java.time.temporal.TemporalUnit
import java.util.concurrent.TimeUnit

class ConfigurableRateLimiterKt(name: String, permitsPerSecond: Int) : RateLimiterKt {

    @Volatile
    private var rateLimiter: io.github.resilience4j.ratelimiter.RateLimiter

    init {
        // AtomicRateLimiter does not distribute events inside refresh period.
        // It releases all available permits immediately on interval start if there is a demand for them.
        // To distribute event in our interval of 1 second we divide it
        // into chunks so that 1 chunk of time is limited to 1 permit.

        // AtomicRateLimiter does not distribute events inside refresh period.
        // It releases all available permits immediately on interval start if there is a demand for them.
        // To distribute event in our interval of 1 second we divide it
        // into chunks so that 1 chunk of time is limited to 1 permit.
        rateLimiter = AtomicRateLimiter(
                name,
                RateLimiterConfig.custom()
                        .limitForPeriod(1)
                        .limitRefreshPeriod(Duration.ofNanos((1000000000 / permitsPerSecond).toLong()))
                        .build()
        )
    }

    override fun close() {
        // Nothing to do
    }

    override fun getRate(): Double {
        val refreshPeriodInNanos = rateLimiter.rateLimiterConfig.limitRefreshPeriodInNanos

        return (1000000000 / refreshPeriodInNanos).toDouble()
    }

    override fun tryAcquire(): Boolean {
        return rateLimiter.getPermission(Duration.ZERO)
    }

    override suspend fun tryAcquire(timeout: Long, unit: TemporalUnit): Boolean {
        return rateLimiter.acquirePermission(Duration.of(timeout, unit))
    }

    override fun updateRate(permitsPerSecond: Int) {
        val name = rateLimiter.name

        // We can update rate limiter at any time because limits for period is always set to 1.
        // So at most we exceed limit by 1

        // We can update rate limiter at any time because limits for period is always set to 1.
        // So at most we exceed limit by 1
        rateLimiter = AtomicRateLimiter(
                name,
                RateLimiterConfig.custom()
                        .limitForPeriod(1)
                        .limitRefreshPeriod(Duration.ofNanos((1000000000 / permitsPerSecond).toLong()))
                        .build()
        )
    }

    private suspend fun io.github.resilience4j.ratelimiter.RateLimiter.acquirePermission(timeout: Duration): Boolean {
        val waitTimeNs = this.reservePermission(timeout)
        when {
            waitTimeNs > 0 -> {
                delay(TimeUnit.NANOSECONDS.toMillis(waitTimeNs))
            }
            waitTimeNs < 0 -> {
                return false
            }
        }
        return true
    }
}