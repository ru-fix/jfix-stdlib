package ru.fix.stdlib.ratelimiter

import org.slf4j.LoggerFactory
import ru.fix.aggregating.profiler.NoopProfiler.NoopProfiledCall
import ru.fix.aggregating.profiler.PrefixedProfiler
import ru.fix.aggregating.profiler.ProfiledCall
import ru.fix.aggregating.profiler.Profiler
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.dynamic.property.api.PropertySubscription
import java.lang.invoke.MethodHandles
import java.time.temporal.ChronoUnit
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer
import java.util.function.Supplier

/**
 * Manages tasks execution with given rate and window.
 * Rate specify how many requests per second will be dispatched.
 * Window size specify how many async operations with uncompleted result allowed.
 * When Window or Rate restriction is reached, dispatcher will stop to process requests and enqueue them in umbound queue.
 * Disaptcher executes all operations in single dedicated thread.
 */
class RateLimitedDispatcherKt(
    name: String,
    rateLimiter: RateLimiter,
    profiler: Profiler,
    windowSize: DynamicProperty<Int>,
    closingTimeout: DynamicProperty<Long>
) : AutoCloseable {

    companion object {
        private const val QUEUE_SIZE_INDICATOR = "queue_size"
        private const val ACTIVE_ASYNC_OPERATIONS = "active_async_operations"
    }

    private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

    private val state = AtomicReference<State>()

    private val rateLimiter: RateLimiter
    private val taskQueue = LinkedBlockingQueue<Task<Any?>>()
    private val commandQueue = LinkedBlockingQueue<Command?>()

    private val thread: Thread
    private val name: String
    private val profiler: Profiler

    /**
     * Accessed only by single dedicated thread
     */
    private var windowSize = 0
    private val closingTimeout: DynamicProperty<Long>

    private val windowSemaphore: Semaphore
    private val windowSizeSubscription: PropertySubscription<Int>

    /**
     * How many async operations executed but their results are not ready yet.
     */
    private val activeAsyncOperations = AtomicInteger()

    /**
     * Creates new dispatcher instance
     *
     * @param name           name of dispatcher - will be used in metrics and worker's thread name
     * @param rateLimiter    rate limiter, which provides rate of operation
     * @param windowSize     ho many async operations with uncompleted result are allowed in dispatcher
     * @param closingTimeout max amount of time (in milliseconds) to wait for pending operations during shutdown.
     *                       If parameter equals 0 then dispatcher will not wait pending operations during closing process.
     *                       Any negative number will be interpreted as 0.
     */
    init {
        this.name = name
        this.rateLimiter = rateLimiter
        this.closingTimeout = closingTimeout
        windowSemaphore = Semaphore(0)

        this.profiler = PrefixedProfiler(profiler, "RateLimiterDispatcher.$name.")

        thread = Thread(TaskProcessor(), "rate-limited-dispatcher-$name")

        state.set(State.RUNNING)
        thread.start()

        this.profiler.attachIndicator(QUEUE_SIZE_INDICATOR) { taskQueue.size.toLong() }
        this.profiler.attachIndicator(ACTIVE_ASYNC_OPERATIONS) { activeAsyncOperations.get().toLong() }

        windowSizeSubscription = windowSize.createSubscription().setAndCallListener { oldValue: Int?, newValue: Int ->
            submitCommand(ChangeWindowSizeCommand(oldValue ?: 0, newValue))
        }
    }

    private fun asyncOperationStarted() {
        activeAsyncOperations.incrementAndGet()
    }

    private fun asyncOperationCompleted() {
        activeAsyncOperations.decrementAndGet()
        windowSemaphore.release()
    }

    private fun submitCommand(command: Command) {
        commandQueue.add(command)
        taskQueue.add(AwakeFromWaitingQueueTask())
    }

    fun <T> compose(supplier: () -> CompletableFuture<T>): CompletableFuture<T> {
        return submit { supplier.invoke().whenComplete { _, _ -> asyncOperationCompleted() } }.thenCompose { cf -> cf }
    }

    fun <AsyncResultT> compose(
        asyncOperation: () -> AsyncResultT,
        asyncResultSubscriber: (asyncResult: AsyncResultT, asyncResultCallback: AsyncResultCallback?) -> Unit
    ): CompletableFuture<AsyncResultT> {
        return submit<AsyncResultT> {
            val asyncResult = asyncOperation.invoke()
            asyncResultSubscriber.invoke(asyncResult, object : AsyncResultCallback {
                override fun onAsyncResultCompleted() {
                    asyncOperationCompleted()
                }
            })
            asyncResult
        }
    }

    /**
     * Submits new operation to task queue.
     *
     *
     * WARNING: task should not be long running operation
     * and should not block processing thread.
     *
     * @param supplier task to execute and retrieve result
     * @return feature which represent result of task execution
     */
    private fun <T> submit(supplier: () -> T): CompletableFuture<T> {
        val result = CompletableFuture<T>()
        val state = state.get()
        if (state != State.RUNNING) {
            val ex = RejectedExecutionException(
                "RateLimiterDispatcher [$name] is in '$state' state"
            )
            result.completeExceptionally(ex)
            return result
        }
        val queueWaitTime = profiler.start("queue_wait")
        taskQueue.add(Task(result, supplier, queueWaitTime) as Task<Any?>)
        return result
    }

    fun updateRate(rate: Int) {
        rateLimiter.updateRate(rate)
    }

//    @kotlin.Throws(Exception::class)
    override fun close() {
        var stateUpdated = state.compareAndSet(State.RUNNING, State.SHUTTING_DOWN)
        if (!stateUpdated) {
            logger.info(
                "Close called on RateLimitedDispatcher [{}] with state [{}]",
                name,
                state.get()
            )
            return
        }
        windowSizeSubscription.close()

        // If queue is empty this will awake waiting Thread
        taskQueue.add(AwakeFromWaitingQueueTask())
        if (closingTimeout.get() < 0) {
            logger.warn(
                "Rate limiter timeout must be greater than or equals 0. Current value is {}, rate limiter name: {}",
                closingTimeout.get(), name
            )
        }
        val timeout = Math.max(closingTimeout.get(), 0)
        if (timeout > 0) {
            thread.join(timeout)
        }
        stateUpdated = state.compareAndSet(State.SHUTTING_DOWN, State.TERMINATE)
        if (!stateUpdated) {
            logger.error(
                "Can't set [TERMINATE] state to RateLimitedDispatcher [{}] in [{}] state",
                name, state.get()
            )
            return
        }
        thread.join()
        rateLimiter.close()
        profiler.detachIndicator(QUEUE_SIZE_INDICATOR)
        profiler.detachIndicator(ACTIVE_ASYNC_OPERATIONS)
    }

    /**
     * Async operation that should be invoked by [RateLimitedDispatcher] at configurate rate.
     * @param <AsyncResultT> represents result of async operation
    </AsyncResultT> */
    @FunctionalInterface
    interface AsyncOperation<AsyncResultT> {
        operator fun invoke(): AsyncResultT
    }

    /**
     * Informs [RateLimitedDispatcher] that async operation result is completed.
     */
    @FunctionalInterface
    interface AsyncResultCallback {
        /**
         * Invoke, when async operation result is completed successfully or with exception.
         */
        fun onAsyncResultCompleted()
    }

    /**
     * Invoked by [RateLimitedDispatcher].
     * Should attach asyncResultCallback to asyncResult and invoke it when asyncResult is complete.
     * @param <AsyncResultT> represents result of async operation
    </AsyncResultT> */
    @FunctionalInterface
    interface AsyncResultSubscriber<AsyncResultT> {
        /**
         * Invoked by [RateLimitedDispatcher].
         * Should attach asyncResultCallback to asyncResult and invoke it when asyncResult is complete.
         *
         * @param asyncResult         that will complete asynchronously
         * @param asyncResultCallback should be invoked when asyncResult is complete
         */
        fun subscribe(asyncResult: AsyncResultT, asyncResultCallback: AsyncResultCallback?)
    }

    private inner class TaskProcessor : Runnable {
        override fun run() {
            while (state.get() == State.RUNNING || state.get() == State.SHUTTING_DOWN && !taskQueue.isEmpty()) {
                try {
                    processCommandsIfExist()
                    waitForTaskInQueueAndProcess()
                } catch (interruptedException: InterruptedException) {
                    logger.error(interruptedException.message, interruptedException)
                    break
                } catch (otherException: Exception) {
                    logger.error(otherException.message, otherException)
                }
            }
            val taskExceptionText: String
            taskExceptionText = if (state.get() == State.TERMINATE) {
                "RateLimitedDispatcher [$name] is in [TERMINATE] state"
            } else {
                "RateLimitedDispatcher [$name] interrupted"
            }
            taskQueue.forEach(Consumer { task: Task<*> ->
                task.future.completeExceptionally(RejectedExecutionException(taskExceptionText))
                task.queueWaitTimeCall.close()
            })
        }

//        @kotlin.Throws(InterruptedException::class)
        private fun processCommandsIfExist() {
            var command: Command? = commandQueue.poll()
            while (command != null) {
                command.apply()
                command = commandQueue.poll()
            }
        }

//        @kotlin.Throws(InterruptedException::class)
        private fun waitForTaskInQueueAndProcess() {
            val task: Task<Any?> = taskQueue.take()
            if (task is AwakeFromWaitingQueueTask) {
                return
            }
            task.queueWaitTimeCall.stop()
            val future = task.future
            try {
                if (windowSize > 0) {
                    profiler.start("acquire_window").use { acquireWindowTime ->
                        var windowAcquired = false
                        while (!windowAcquired) {
                            if (state.get() == State.TERMINATE) {
                                rejectDueToTerminateState(future)
                                return
                            }
                            windowAcquired = windowSemaphore.tryAcquire(3, TimeUnit.SECONDS)
                        }
                        acquireWindowTime.stop()
                    }
                }
                profiler.start("acquire_limit").use { limitAcquireTime ->
                    var limitAcquired = false
                    while (!limitAcquired) {
                        if (state.get() == State.TERMINATE) {
                            rejectDueToTerminateState(future)
                            return
                        }
                        limitAcquired = rateLimiter.tryAcquire(3, ChronoUnit.SECONDS)
                    }
                    limitAcquireTime.stop()
                }

                // Since async operation may complete faster then Started method call
                // it must be called before asynchronous operation started
                asyncOperationStarted()
                val result: Any = profiler.profile(
                    "supply_operation",
                    Supplier { task.supplier.invoke() }
                )!!

                future.complete(result)
            } catch (e: Exception) {
                future.completeExceptionally(e)
            }
        }

        private fun rejectDueToTerminateState(future: CompletableFuture<*>) {
            future.completeExceptionally(
                RejectedExecutionException(
                    "RateLimitedDispatcher [$name] is in [TERMINATE] state"
                )
            )
        }
    }

    private open class Task<T>(
        val future: CompletableFuture<T>,
        val supplier: () -> T,
        val queueWaitTimeCall: ProfiledCall
    ) {
//        fun getSupplier(): () -> T {
//            return supplier
//        }

//        fun getFuture(): CompletableFuture<T> {
//            return future
//        }

//        fun getQueueWaitTimeCall(): ProfiledCall {
//            return queueWaitTimeCall
//        }
    }

    private class AwakeFromWaitingQueueTask : Task<Any?> (
        CompletableFuture(),
        { null },
        NoopProfiledCall()
    )

    private interface Command {
//        @kotlin.Throws(InterruptedException::class)
        fun apply()
    }

    private inner class ChangeWindowSizeCommand(private val oldSize: Int, private val newSize: Int) :
        Command {
//        @kotlin.Throws(InterruptedException::class)
        override fun apply() {
            if (newSize == oldSize) return
            windowSize = newSize
            if (newSize > oldSize) {
                windowSemaphore.release(newSize - oldSize)
            } else {
                windowSemaphore.acquire(oldSize - newSize)
            }
        }
    }

    private enum class State {
        RUNNING,
        SHUTTING_DOWN,
        TERMINATE
    }

}