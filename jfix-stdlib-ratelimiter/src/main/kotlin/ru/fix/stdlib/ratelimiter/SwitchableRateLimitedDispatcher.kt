package ru.fix.stdlib.ratelimiter

import java.util.concurrent.CompletableFuture
import java.util.function.Supplier

class SwitchableRateLimitedDispatcher(
    private val rateLimitedDispatcherProvider: RateLimitedDispatcherProvider
) : RateLimitedDispatcherInterface {

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
