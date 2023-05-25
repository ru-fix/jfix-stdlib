package ru.fix.stdlib.ratelimiter

import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext

class RateLimitedDispatcherProviderImpl(
    private val rateLimiterFactory: RateLimiterFactory,
    private val rateLimiterSettings: DynamicProperty<RateLimiterSettings>,
    private val profiler: Profiler,
    private val context: CoroutineContext,
    private val limiterId: String,
) : RateLimitedDispatcherProvider {

    private var rateLimitedDispatcher: AtomicReference<RateLimitedDispatcherInterface?> = AtomicReference(null)
    private var isSuspendable = AtomicReference(rateLimiterSettings.get().isSuspendable)

    init {
        rateLimiterSettings.map { it.isSuspendable }
            .createSubscription()
            .setAndCallListener { _, newValue ->
                if (isSuspendable.get() != newValue) {
                    updateDispatcher(newValue)
                }
            }
        updateDispatcher(isSuspendable.get())
    }

    @Synchronized
    override fun provideDispatcher(): RateLimitedDispatcherInterface = rateLimitedDispatcher.get()!!

    @Synchronized
    private fun updateDispatcher(newValue: Boolean) {
        rateLimitedDispatcher.get()?.close()
        rateLimitedDispatcher.set(
            if (newValue) {
                createSuspendableRateLimitedDispatcher()
            } else {
                createRateLimitedDispatcher()
            }
        )
        isSuspendable.set(newValue)
    }

    private fun createSuspendableRateLimitedDispatcher(): SuspendableRateLimitedDispatcher {
        val rateLimiter: RateLimiterKt = rateLimiterFactory.createLimiterKt(
            limiterId,
            rateLimiterSettings.map { it.limitPerSec },
        )
        return SuspendableRateLimitedDispatcher(
            limiterId,
            rateLimiter,
            profiler,
            rateLimiterSettings.map { it.closeTimeout.toMillis() },
            context,
        )
    }

    private fun createRateLimitedDispatcher(): RateLimitedDispatcher {
        val rateLimiter: RateLimiter = rateLimiterFactory.createLimiter(
            limiterId,
            rateLimiterSettings.map { it.limitPerSec },
        )
        return RateLimitedDispatcher(
            limiterId,
            rateLimiter,
            profiler,
            rateLimiterSettings.map { it.closeTimeout.toMillis() },
        )
    }

}