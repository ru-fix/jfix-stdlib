package ru.fix.stdlib.ratelimiter

import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
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

    companion object {
        private const val AWAIT_MS = 1_000L
    }

    private var rateLimitedDispatcher: AtomicReference<RateLimitedDispatcherInterface?> = AtomicReference(null)
    private var isSuspendable = AtomicReference(rateLimiterSettings.get().isSuspendable)
    private var countDownLatch = CountDownLatch(1)

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
        countDownLatch.await(AWAIT_MS, TimeUnit.MILLISECONDS)
        return rateLimitedDispatcher.get()!!.compose(supplier)
    }

    override fun updateRate(rate: Int) {
        countDownLatch.await(AWAIT_MS, TimeUnit.MILLISECONDS)
        return rateLimitedDispatcher.get()!!.updateRate(rate)
    }

    override fun close() {
        countDownLatch.await(AWAIT_MS, TimeUnit.MILLISECONDS)
        return rateLimitedDispatcher.get()!!.close()
    }

    private fun updateDispatcher(newValue: Boolean) {
        countDownLatch = CountDownLatch(1)
        rateLimitedDispatcher.get()?.close()
        rateLimitedDispatcher.set(
            if (newValue) {
                createSuspendableRateLimitedDispatcher()
            } else {
                createRateLimitedDispatcher()
            }
        )
        isSuspendable.set(newValue)
        countDownLatch.countDown()
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