package ru.fix.stdlib.ratelimiter

import kotlinx.coroutines.runBlocking
import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext

class RateLimitedDispatcherProvider(
    private val rateLimiterFactory: RateLimiterFactory,
    private val rateLimiterSettings: DynamicProperty<RateLimiterSettings>,
    private val profiler: Profiler,
    private val context: CoroutineContext,
    private val limiterId: String,
) {

    private var suspendableRateLimitedDispatcher: AtomicReference<SuspendableRateLimitedDispatcher> = AtomicReference()
    private var rateLimitedDispatcher: AtomicReference<RateLimitedDispatcher> = AtomicReference()

    fun provideDispatcher(): RateLimitedDispatcherInterface {
        return if (rateLimiterSettings.map { it.isSuspendable }.get()) {
            getSuspendableRateLimitedDispatcher()
        } else {
            getRateLimitedDispatcher()
        }
    }

    private fun getSuspendableRateLimitedDispatcher(): SuspendableRateLimitedDispatcher {
        return suspendableRateLimitedDispatcher.get() ?: runBlocking {
            val rateLimiter: RateLimiterKt = rateLimiterFactory.createLimiterKt(
                limiterId,
                rateLimiterSettings.map { it.limitPerSec },
            )
            suspendableRateLimitedDispatcher.set(
                SuspendableRateLimitedDispatcher(
                    limiterId,
                    rateLimiter,
                    profiler,
                    rateLimiterSettings.map { it.closeTimeout.toMillis() },
                    context,
                ),
            )
            suspendableRateLimitedDispatcher.get()
        }
    }

    private fun getRateLimitedDispatcher(): RateLimitedDispatcher {
        return rateLimitedDispatcher.get() ?: runBlocking {
            val rateLimiter: RateLimiter = rateLimiterFactory.createLimiter(
                limiterId,
                rateLimiterSettings.map { it.limitPerSec },
            )
            rateLimitedDispatcher.set(
                RateLimitedDispatcher(
                    limiterId,
                    rateLimiter,
                    profiler,
                    rateLimiterSettings.map { it.closeTimeout.toMillis() },
                ),
            )
            rateLimitedDispatcher.get()
        }
    }

    data class RateLimiterSettings(
        val limitPerSec: Double,
        val closeTimeout: Duration,
        val isSuspendable: Boolean,
    )

}