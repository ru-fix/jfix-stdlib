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
) : RateLimitedDispatcherProvider, AutoCloseable {

    private val rateLimitedDispatcherSuspendable: SuspendableRateLimitedDispatcher by lazy {
        createSuspendableRateLimitedDispatcher()
    }

    private val rateLimitedDispatcher: RateLimitedDispatcher by lazy {
        createRateLimitedDispatcher()
    }

    private var dispatcherReference: AtomicReference<RateLimitedDispatcherInterface?> = AtomicReference(null)
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

    override fun provideDispatcher(): RateLimitedDispatcherInterface = dispatcherReference.get()!!

    override fun close() {
        rateLimitedDispatcher.close()
        rateLimitedDispatcherSuspendable.close()
    }

    private fun updateDispatcher(newValue: Boolean) {
        dispatcherReference.set(
            if (newValue) {
                rateLimitedDispatcherSuspendable
            } else {
                rateLimitedDispatcher
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