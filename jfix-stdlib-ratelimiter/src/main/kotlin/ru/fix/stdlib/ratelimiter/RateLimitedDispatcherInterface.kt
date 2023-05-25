package ru.fix.stdlib.ratelimiter

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import java.util.concurrent.CompletableFuture
import java.util.function.Supplier

interface RateLimitedDispatcherInterface: AutoCloseable {

    suspend fun <T: Any?> decorateSuspend(supplier: suspend () -> T): T = compose {
        GlobalScope.future(Dispatchers.Unconfined) {
            supplier()
        }
    }.await()

    fun <T> compose(supplier: Supplier<CompletableFuture<T>>): CompletableFuture<T>

    fun updateRate(rate: Int)

}
