package ru.fix.stdlib.ratelimiter

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.util.concurrent.CompletableFuture

class RateLimitedDispatcherExtended(
    name: String?,
    rateLimiter: RateLimiter?,
    profiler: Profiler?,
    windowSize: DynamicProperty<Int>?,
    closingTimeout: DynamicProperty<Long>?
) : RateLimitedDispatcher(name, rateLimiter, profiler, windowSize, closingTimeout), RateLimitedDispatcherInterface {

    constructor(
        name: String?,
        rateLimiter: RateLimiter?,
        profiler: Profiler?,
        closingTimeout: DynamicProperty<Long>?
    ) : this(name, rateLimiter, profiler, DynamicProperty.of(0), closingTimeout)


    override suspend fun <T: Any?> decorateSuspend(supplier: suspend () -> T): T = compose {
        GlobalScope.future(Dispatchers.Unconfined) {
            supplier()
        }
    }.await()

    override fun <T> compose(supplier: () -> CompletableFuture<T>): CompletableFuture<T> {
        return super.compose(supplier)
    }

    override fun updateRate(rate: Int) {
        super.updateRate(rate)
    }
}
