package ru.fix.stdlib.ratelimiter

import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier
import kotlin.coroutines.CoroutineContext

class SwitchableRateLimitedDispatcher(
    private val name: String,
    private val rateLimiterKt: RateLimiterKt,
    private val rateLimiter: RateLimiter,
    private val profiler: Profiler,
    private val context: CoroutineContext,
    private val rateLimiterSettings: DynamicProperty<RateLimiterSettings>,
) : RateLimitedDispatcherInterface {

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

    override fun <T> compose(supplier: Supplier<CompletableFuture<T>>): CompletableFuture<T> {
        return dispatcherReference.get()!!.compose(supplier)
    }

    override suspend fun <T> decorateSuspend(supplier: suspend () -> T): T {
        return dispatcherReference.get()!!.decorateSuspend(supplier)
    }

    override fun updateRate(rate: Int) {
        return dispatcherReference.get()!!.updateRate(rate)
    }

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
        return SuspendableRateLimitedDispatcher(
            name,
            rateLimiterKt,
            profiler,
            rateLimiterSettings.map { it.closeTimeout.toMillis() },
            context,
        )
    }

    private fun createRateLimitedDispatcher(): RateLimitedDispatcher {
        return RateLimitedDispatcher(
            name,
            rateLimiter,
            profiler,
            rateLimiterSettings.map { it.closeTimeout.toMillis() },
        )
    }

}