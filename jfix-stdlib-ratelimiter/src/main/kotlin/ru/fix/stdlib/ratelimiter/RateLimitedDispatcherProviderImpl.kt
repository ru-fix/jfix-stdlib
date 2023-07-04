package ru.fix.stdlib.ratelimiter

import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext

class RateLimitedDispatcherProviderImpl(
    private val rateLimiterFactory: RateLimiterFactory,
    private val rateLimiterSettings: DynamicProperty<RateLimiterSettings>,
    private val profiler: Profiler,
    private val suspendableWaitingScopeContext: CoroutineContext,
    private val limiterId: String,
    private val window: DynamicProperty<Int> = DynamicProperty.of(0),
) : RateLimitedDispatcherProvider, AutoCloseable {

    private val rateLimitedDispatcherSuspendable: Lazy<SuspendableRateLimitedDispatcher> = lazy {
        createSuspendableRateLimitedDispatcher()
    }

    private val rateLimitedDispatcher: Lazy<RateLimitedDispatcher> = lazy {
        createRateLimitedDispatcher()
    }

    private var useSuspendable = AtomicReference(rateLimiterSettings.get().isSuspendable)

    init {
        rateLimiterSettings.map { it.isSuspendable }
            .createSubscription()
            .setAndCallListener { oldValue, newValue ->
                updateDispatcher(newValue ?: oldValue)
            }
    }

    override fun provideDispatcher(): RateLimitedDispatcherInterface =
        if (useSuspendable.get()) rateLimitedDispatcherSuspendable.value else rateLimitedDispatcher.value


    override fun close() {
        if (rateLimitedDispatcher.isInitialized()) rateLimitedDispatcher.value.close()
        if (rateLimitedDispatcherSuspendable.isInitialized()) rateLimitedDispatcherSuspendable.value.close()
    }

    private fun updateDispatcher(newValue: Boolean) {
        if (newValue) rateLimitedDispatcherSuspendable.value else rateLimitedDispatcher.value
        useSuspendable.set(newValue)
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
            suspendableWaitingScopeContext,
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
            window,
            rateLimiterSettings.map { it.closeTimeout.toMillis() },
        )
    }

}