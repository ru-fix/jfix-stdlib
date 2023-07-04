package ru.fix.stdlib.ratelimiter

import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.CompletableFuture
import java.util.function.Supplier
import kotlin.coroutines.CoroutineContext

class SwitchableRateLimitedDispatcher(
    profiler: Profiler,
    suspendableWaitingScopeContext: CoroutineContext,
    rateLimiterSettings: DynamicProperty<RateLimiterSettings>,
    rateLimiterFactory: RateLimiterFactory,
    limiterId: String,
    window: DynamicProperty<Int> = DynamicProperty.of(0),
) : RateLimitedDispatcherInterface {

    private val rateLimitedDispatcherProvider: RateLimitedDispatcherProvider = RateLimitedDispatcherProviderImpl(
        rateLimiterFactory = rateLimiterFactory,
        rateLimiterSettings = rateLimiterSettings,
        profiler = profiler,
        suspendableWaitingScopeContext = suspendableWaitingScopeContext,
        limiterId = limiterId,
        window = window
    )

    override fun <T> compose(supplier: Supplier<CompletableFuture<T>>): CompletableFuture<T> {
        return rateLimitedDispatcherProvider.provideDispatcher().compose(supplier)
    }

    override suspend fun <T> decorateSuspend(supplier: suspend () -> T): T {
        return rateLimitedDispatcherProvider.provideDispatcher().decorateSuspend(supplier)
    }

    override fun updateRate(rate: Int) {
        return rateLimitedDispatcherProvider.provideDispatcher().updateRate(rate)
    }

    override fun close() {
        return rateLimitedDispatcherProvider.provideDispatcher().close()
    }

}