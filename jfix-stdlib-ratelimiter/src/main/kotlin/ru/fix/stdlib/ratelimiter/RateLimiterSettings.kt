package ru.fix.stdlib.ratelimiter

import java.time.Duration

data class RateLimiterSettings(
    val limitPerSec: Double,
    val closeTimeout: Duration,
    val isSuspendable: Boolean,
)
