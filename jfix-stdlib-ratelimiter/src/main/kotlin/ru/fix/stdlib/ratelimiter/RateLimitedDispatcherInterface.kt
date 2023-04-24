package ru.fix.stdlib.ratelimiter

import java.util.concurrent.CompletableFuture

interface RateLimitedDispatcherInterface {

    suspend fun <T> decorateSuspend(supplier: suspend () -> T): T

    fun <T> compose(supplier: () -> CompletableFuture<T>): CompletableFuture<T>

    fun updateRate(rate: Int)

}
