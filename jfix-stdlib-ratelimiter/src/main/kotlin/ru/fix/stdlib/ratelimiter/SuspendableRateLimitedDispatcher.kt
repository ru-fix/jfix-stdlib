package ru.fix.stdlib.ratelimiter

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import ru.fix.aggregating.profiler.PrefixedProfiler
import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import java.time.temporal.ChronoUnit
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Supplier
import kotlin.coroutines.CoroutineContext

/**
 * Manages tasks execution with given rate and window.
 * Rate specify how many requests per second will be dispatched.
 * When Rate restriction is reached, dispatcher will stop to process requests.
 * Dispatcher executes all operations in single dedicated thread.
 *
 * @param name           name of dispatcher - will be used in metrics and worker's thread name
 * @param rateLimiter    rate limiter, which provides rate of operation
 * @param closingTimeout max amount of time (in milliseconds) to wait for pending operations during shutdown.
 *                       If parameter equals 0 then dispatcher will not wait pending operations during closing process.
 *                       Any negative number will be interpreted as 0.
 *
 */
class SuspendableRateLimitedDispatcher(
        private val name: String,
        private val rateLimiter: RateLimiter,
        profiler: Profiler,
        closingTimeout: DynamicProperty<Long>,
        context: CoroutineContext
) : AutoCloseable {

    companion object {
        private const val ACTIVE_ASYNC_OPERATIONS = "active_async_operations"

        private val logger = KotlinLogging.logger {}
    }

    private val state = AtomicReference<State>()

    private val profiler: Profiler

    /**
     * How many async operations executed but their results are not ready yet.
     */
    private val activeAsyncOperations = AtomicInteger()

    private val waitingScope: DispatcherWaitingScope = DispatcherWaitingScope(closingTimeout, context)

    /**
     * Creates new dispatcher instance
     */
    init {
        this.profiler = PrefixedProfiler(profiler, "RateLimiterDispatcher.$name.")

        state.set(State.RUNNING)

        this.profiler.attachIndicator(ACTIVE_ASYNC_OPERATIONS) { activeAsyncOperations.get().toLong() }
    }

    private fun asyncOperationStarted() {
        activeAsyncOperations.incrementAndGet()
    }

    private fun asyncOperationCompleted() {
        activeAsyncOperations.decrementAndGet()
    }

    suspend fun <T> compose(supplier: suspend () -> T): T {
        return submit {
            val res = supplier.invoke()
            asyncOperationCompleted()
            res
        }.await()
    }

    /**
     * Submits new operation to task queue.
     *
     *
     * WARNING: task should not be long-running operation
     * and should not block processing thread.
     *
     * @param supplier task to execute and retrieve result
     * @return feature which represent result of task execution
     */
    private suspend fun <T> submit(supplier: suspend () -> T): Deferred<T> {
        val state = state.get()
        return if (state != State.RUNNING) {
            throw RejectedExecutionException(
                    "RateLimiterDispatcher [$name] is in '$state' state"
            )
        } else {
            waitingScope.async {
                processTask(supplier)
            }
        }
    }

    fun updateRate(rate: Int) {
        rateLimiter.updateRate(rate)
    }

    override fun close() {
        var stateUpdated = state.compareAndSet(State.RUNNING, State.SHUTTING_DOWN)
        if (!stateUpdated) {
            logger.info("Close called on RateLimitedDispatcher [{}] with state [{}]", name, state.get())
            return
        }

        if (!waitingScope.waitChildrenAndCancel()) {
            logger.warn("Timeout while waiting for coroutines finishing")
            throw RejectedExecutionException("RateLimitedDispatcher [$name] get timeout while waiting for coroutines finishing")
        }
        stateUpdated = state.compareAndSet(State.SHUTTING_DOWN, State.TERMINATE)
        if (!stateUpdated) {
            logger.error(
                "Can't set [TERMINATE] state to RateLimitedDispatcher [{}] in [{}] state",
                name, state.get()
            )
            return
        }
        rateLimiter.close()
        profiler.detachIndicator(ACTIVE_ASYNC_OPERATIONS)
    }


    private suspend fun <T> processTask(supplier: suspend () -> T): T {
        try {
            profiler.start("acquire_limit").use { limitAcquireTime ->
                var limitAcquired = false
                while (!limitAcquired) {
                    if (state.get() == State.TERMINATE) {
                        val message = "RateLimitedDispatcher [$name] received TERMINATE state while rate limiter acquiring"
                        logger.error(message)
                        throw RejectedExecutionException(message)
                    }
                    limitAcquired = rateLimiter.tryAcquire(3, ChronoUnit.SECONDS)
                }
                limitAcquireTime.stop()
            }

            // Since async operation may complete faster than Started method call
            // it must be called before asynchronous operation started
            asyncOperationStarted()

            return profiler.profile(
                    "supply_operation",
                    Supplier { runBlocking { supplier.invoke() } }
            )!!
        } catch (e: Exception) {
            logger.error("RateLimitedDispatcher [$name] received exception: $e")
            throw RejectedExecutionException("RateLimitedDispatcher [$name] received exception: ${e.message}")
        }
    }

    private enum class State {
        RUNNING,
        SHUTTING_DOWN,
        TERMINATE
    }

}