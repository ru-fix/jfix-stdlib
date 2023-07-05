package ru.fix.stdlib.ratelimiter

import io.kotest.assertions.assertSoftly
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.doubles.shouldBeBetween
import io.kotest.matchers.shouldBe
import io.kotest.matchers.throwable.shouldHaveCause
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import kotlinx.coroutines.runBlocking
import mu.KLogging
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertTimeoutPreemptively
import org.junit.jupiter.api.parallel.Execution
import org.junit.jupiter.api.parallel.ExecutionMode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.aggregating.profiler.Profiler
import ru.fix.aggregating.profiler.ProfilerReport
import ru.fix.dynamic.property.api.AtomicProperty
import ru.fix.dynamic.property.api.DynamicProperty
import ru.fix.stdlib.ratelimiter.enums.DispatcherType
import java.lang.Thread.sleep
import java.time.Duration
import java.util.concurrent.*
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Execution(ExecutionMode.CONCURRENT)
class RateLimitedDispatcherTest {
    private companion object : KLogging() {
        const val DISPATCHER_NAME = "dispatcher-name"
        const val DISPATCHER_METRICS_PREFIX = "RateLimiterDispatcher.$DISPATCHER_NAME"
    }

    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `dispatch async operation with user defined async result type, operation invoked and it's result returned`(
        dispatcherType: DispatcherType
    ) {
        class UserAsyncResult {
            fun whenComplete(callback: () -> Unit) {
                callback()
            }
        }

        val dispatcher = createDispatcher(dispatcherType)

        val asyncResultInstance = UserAsyncResult()

        fun userAsyncOperation(): UserAsyncResult {
            return asyncResultInstance
        }

        val delayedSubmission = dispatcher.compose(
                { userAsyncOperation() },
                { asyncResult, callback -> asyncResult.whenComplete { callback.onAsyncResultCompleted() } }
        )

        (asyncResultInstance === delayedSubmission.get()).shouldBeTrue()

        dispatcher.close()
    }

    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `dispatch async operation with successfull CompletableFuture, operation invoked and it's result returned`(
        dispatcherType: DispatcherType
    ) {
        val dispatcher = createDispatcher(dispatcherType)

        val operationResult = Object()
        fun userAsyncOperation(): CompletableFuture<Any> {
            return completedFuture(operationResult)
        }

        val delayedSubmissionFuture = dispatcher.compose { userAsyncOperation() }

        (operationResult === delayedSubmissionFuture.get()).shouldBeTrue()

        dispatcher.close()
    }


    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `dispatch async operation with exceptional CompletableFuture, operation invoked and it's result returned`(
        dispatcherType: DispatcherType
    ) {
        val dispatcher = createDispatcher(dispatcherType)

        val asyncOperationException = Exception("some error")

        fun userAsyncOperation(): CompletableFuture<Any> {
            return CompletableFuture<Any>().apply {
                completeExceptionally(asyncOperationException)
            }
        }

        val delayedSubmissionFuture = dispatcher.compose { userAsyncOperation() }

        await().atMost(Duration.ofSeconds(10)).until {
            delayedSubmissionFuture.isCompletedExceptionally
        }

        val actualException = shouldThrow<Exception> { delayedSubmissionFuture.get() }

        actualException.shouldHaveCause()
        actualException.cause.shouldBe(asyncOperationException)

        dispatcher.close()
    }


    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `if windows size is 0, then restricted only by limiter`(
        dispatcherType: DispatcherType
    ) {
        `async operations are restricted by limiter limit `(0, dispatcherType)
    }


    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `if window size is not empty and quite big, restricted by limiter`(
        dispatcherType: DispatcherType
    ) {
        `async operations are restricted by limiter limit `(100_000, dispatcherType)
    }

    private fun `async operations are restricted by limiter limit `(windowSize: Int, dispatcherType: DispatcherType) {
        val ratePerSecond = 500
        val iterations = 5 * ratePerSecond

        val report = `submit series of operations`(
                ratePerSecond = ratePerSecond,
                iterations = iterations,
                windowSize = DynamicProperty.of(windowSize),
                dispatcherType = dispatcherType
        )

        val operationReport = report.profilerCallReports.single { it.identity.name == "operation" }

        logger.info("Throughput $operationReport")
        operationReport.stopThroughputAvg.shouldBeBetween(
                ratePerSecond.toDouble(),
                ratePerSecond.toDouble(),
                ratePerSecond.toDouble() * 0.25)
    }

    private fun `submit series of operations`(
            ratePerSecond: Int,
            iterations: Int,
            windowSize: DynamicProperty<Int>,
            dispatcherType: DispatcherType
    ): ProfilerReport {


        val profiler = AggregatingProfiler()

        val dispatcher = createDispatcher(
                rateLimitRequestPerSecond = ratePerSecond,
                window = windowSize,
                profiler = profiler,
                dispatcherType = dispatcherType
        )

        val counter = AtomicInteger(0)


        val profilerReporter = profiler.createReporter()
        val profiledCall = profiler.profiledCall("operation")

        val features = List(iterations) {
            dispatcher.compose {
                profiledCall.profile<CompletableFuture<Int>> {
                    completedFuture(counter.incrementAndGet())
                }
            }
        }

        logger.info("Submit $iterations operations.")
        features.forEach { it.join() }

        counter.get().shouldBe(iterations)
        features.map { it.join() }.toSet().containsAll((1..iterations).toList())

        val report = profilerReporter.buildReportAndReset()
        dispatcher.close()

        return report;
    }

    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `when window of uncompleted operations is full no new operation is dispatched`(
        dispatcherType: DispatcherType
    ) {
        val dispatcher = TrackableDispatcher(dispatcherType)
        dispatcher.windowProperty.set(10)

        dispatcher.submitTasks(1..11)

        sleep(4000)

        dispatcher.isSubmittedTaskInvoked(1..10).shouldBeTrue()
        dispatcher.isSubmittedTaskInvoked(11).shouldBeFalse()

        dispatcher.completeTask(4)

        await().atMost(Duration.ofSeconds(10)).until {
            dispatcher.isSubmittedTaskInvoked(11)
        }

        dispatcher.completeAllAndClose()
    }

    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `'queue_wait', 'acquire_limit', 'acquire_window', 'supplied_operation', 'queue_size', 'active_async_operations' metrics gathered during execution`(
        dispatcherType: DispatcherType
    ) {

        val ratePerSecond = 500
        val iterations = 5 * ratePerSecond

        val report = `submit series of operations`(
                ratePerSecond = ratePerSecond,
                iterations = iterations,
                windowSize = DynamicProperty.of(100),
                dispatcherType = dispatcherType
        )

        report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.queue_wait" }
                .stopSum.shouldBe(iterations)

        report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.acquire_window" }
                .stopSum.shouldBe(iterations)

        report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.acquire_limit" }
                .stopSum.shouldBe(iterations)

        report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.supply_operation" }
                .stopSum.shouldBe(iterations)

        report.indicators.map { it.key.name }.shouldContain("$DISPATCHER_METRICS_PREFIX.queue_size")

        report.indicators.map { it.key.name }.shouldContain("$DISPATCHER_METRICS_PREFIX.active_async_operations")

        logger.info { "Resulting profiler report: $report" }
    }

    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `indicators 'queue_size' and 'active_async_operations' adjusted according to number of queued and active operations`(
        dispatcherType: DispatcherType
    ) {
        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()
        val trackableDispatcher = TrackableDispatcher(dispatcherType, profiler)

        trackableDispatcher.windowProperty.set(10)
        trackableDispatcher.submitTasks(1..12)
        await().atMost(1, TimeUnit.SECONDS).until {
            trackableDispatcher.isSubmittedTaskInvoked(1..10)
        }
        reporter.buildReportAndReset().assertSoftly {
            indicators.mapKeys { it.key.name }.assertSoftly {
                it["$DISPATCHER_METRICS_PREFIX.queue_size"] shouldBe 1
                it["$DISPATCHER_METRICS_PREFIX.active_async_operations"] shouldBe 10
            }
            profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.acquire_window" }
                    .activeCallsCountMax shouldBe 1
        }

        trackableDispatcher.completeTasks(1..10)
        await().atMost(1, TimeUnit.SECONDS).until {
            trackableDispatcher.isSubmittedTaskInvoked(11..12)
        }
        reporter.buildReportAndReset().indicators.mapKeys { it.key.name }.assertSoftly {
            it["$DISPATCHER_METRICS_PREFIX.queue_size"] shouldBe 0
            it["$DISPATCHER_METRICS_PREFIX.active_async_operations"] shouldBe 2
        }

        trackableDispatcher.completeTasks(11..12)
        reporter.buildReportAndReset().indicators.mapKeys { it.key.name }.assertSoftly {
            it["$DISPATCHER_METRICS_PREFIX.queue_size"] shouldBe 0
            it["$DISPATCHER_METRICS_PREFIX.active_async_operations"] shouldBe 0
        }

        trackableDispatcher.completeAllAndClose()
    }

    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `WHEN many fast CompletableFuture tasks completed THEN 'active_async_operations' is 0`(
        dispatcherType: DispatcherType
    ) {
        val report = `submit series of operations`(500, 4000, DynamicProperty.of(1000), dispatcherType)

        logger.info { report }
        report.indicators.mapKeys { it.key.name }.assertSoftly {
            it["$DISPATCHER_METRICS_PREFIX.active_async_operations"] shouldBe 0
        }
    }

    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `WHEN completed futures arrived THEN indicators are correctly adjusted`(
        dispatcherType: DispatcherType
    ) {
        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()
        val trackableDispatcher = TrackableDispatcher(dispatcherType, profiler)

        trackableDispatcher.windowProperty.set(5)
        trackableDispatcher.submitCompletedTasks(1..4)
        trackableDispatcher.submitTasks(5..6)
        await().atMost(1, TimeUnit.SECONDS).until {
            trackableDispatcher.isSubmittedTaskInvoked(1..6)
        }
        reporter.buildReportAndReset().indicators.mapKeys { it.key.name }.assertSoftly {
            it["$DISPATCHER_METRICS_PREFIX.queue_size"] shouldBe 0
            it["$DISPATCHER_METRICS_PREFIX.active_async_operations"] shouldBe 2
        }

        trackableDispatcher.submitCompletedTasks(7..10)
        await().atMost(1, TimeUnit.SECONDS).until {
            trackableDispatcher.isSubmittedTaskInvoked(7..10)
        }
        reporter.buildReportAndReset().indicators.mapKeys { it.key.name }.assertSoftly {
            it["$DISPATCHER_METRICS_PREFIX.queue_size"] shouldBe 0
            it["$DISPATCHER_METRICS_PREFIX.active_async_operations"] shouldBe 2
        }

        trackableDispatcher.completeTasks(5..6)
        reporter.buildReportAndReset().indicators.mapKeys { it.key.name }.assertSoftly {
            it["$DISPATCHER_METRICS_PREFIX.queue_size"] shouldBe 0
            it["$DISPATCHER_METRICS_PREFIX.active_async_operations"] shouldBe 0
        }

        trackableDispatcher.completeAllAndClose()
    }

    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `increasing window size allows to submit new operations up to the new limit`(
        dispatcherType: DispatcherType
    ) {

        val trackableDispatcher = TrackableDispatcher(dispatcherType)
        trackableDispatcher.windowProperty.set(10)

        trackableDispatcher.submitTasks(1..11)

        sleep(4000)
        trackableDispatcher.isSubmittedTaskInvoked(1..10).shouldBeTrue()
        trackableDispatcher.isSubmittedTaskInvoked(11).shouldBeFalse()

        trackableDispatcher.windowProperty.set(11)

        trackableDispatcher.submitTask(12)
        trackableDispatcher.completeTask(1)

        await().atMost(Duration.ofSeconds(10)).until {
            trackableDispatcher.isSubmittedTaskInvoked(1..12)
        }

        trackableDispatcher.completeAllAndClose();
    }


    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `decreasing window size reduces limit`(dispatcherType: DispatcherType) {
        val trackableDispatcher = TrackableDispatcher(dispatcherType)
        trackableDispatcher.windowProperty.set(10)

        trackableDispatcher.submitTasks(1..10)

        sleep(4000)
        trackableDispatcher.isSubmittedTaskInvoked(1..10).shouldBeTrue()
        trackableDispatcher.completeTasks(1..10)

        trackableDispatcher.windowProperty.set(4)
        trackableDispatcher.submitTasks(11..15)
        sleep(4000)

        trackableDispatcher.isSubmittedTaskInvoked(11..14).shouldBeTrue()
        trackableDispatcher.isSubmittedTaskInvoked(15).shouldBeFalse()

        trackableDispatcher.completeTask(11)
        await().atMost(Duration.ofSeconds(10)).until {
            trackableDispatcher.isSubmittedTaskInvoked(15)
        }

        trackableDispatcher.completeAllAndClose();
    }


    /**
     * When dispatcher closingTimeout is enough for pending tasks to complete
     * such tasks will complete normally
     */
    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `on shutdown fast tasks complete normally`(dispatcherType: DispatcherType) {

        val dispatch = createDispatcher(closingTimeout = 5_000, dispatcherType = dispatcherType)

        assertTimeoutPreemptively(Duration.ofSeconds(10)) {

            val blockingTaskIsStarted = CountDownLatch(1)


            dispatch.compose {
                blockingTaskIsStarted.countDown()
                //Due to blocking nature of dispatch.close we hae to use sleep
                Thread.sleep(1000)
                completedFuture(true)
            }

            val futures = List(3) {
                dispatch.compose {
                    completedFuture(true)
                }
            }

            blockingTaskIsStarted.await()
            dispatch.close()

            CompletableFuture.allOf(*futures.toTypedArray()).exceptionally { null }.join()

            futures.forEach { future: CompletableFuture<*> ->
                future.isDone.shouldBeTrue()
                future.isCompletedExceptionally.shouldBeFalse()
            }
        }
    }

    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `on shutdown slow tasks complete exceptionally`(dispatcherType: DispatcherType) {
        val dispatch = createDispatcher(closingTimeout = 0, dispatcherType = dispatcherType)

        assertTimeoutPreemptively(Duration.ofSeconds(5)) {
            val blockingTaskIsStarted = CountDownLatch(1)

            dispatch.compose {
                blockingTaskIsStarted.countDown()
                //Due to blocking nature of dispatch.close we hae to use sleep
                Thread.sleep(1000)
                completedFuture(true)
            }

            val futures = ArrayList<CompletableFuture<*>>()
            for (i in 1..3) {
                futures.add(dispatch.compose {
                    completedFuture(true)
                })
            }

            blockingTaskIsStarted.await()
            dispatch.close()

            CompletableFuture.allOf(*futures.toTypedArray()).exceptionally { null }.join()

            futures.forEach { future: CompletableFuture<*> ->
                future.isDone.shouldBeTrue()
                future.isCompletedExceptionally.shouldBeTrue()
                shouldThrow<ExecutionException> { future.get() }
                        .cause.shouldBeInstanceOf<RejectedExecutionException>()
            }
        }
    }

    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `task, submitted in closed dispatcher, is rejected with exception`(dispatcherType: DispatcherType) {
        val dispatcher = createDispatcher(dispatcherType)
        dispatcher.close()

        val result = dispatcher.compose {
            completedFuture(true)
        }

        await().atMost(Duration.ofSeconds(2)).until {
            result.isCompletedExceptionally
        }
        shouldThrow<ExecutionException> { result.get() }
                .cause.shouldBeInstanceOf<RejectedExecutionException>()
    }

    private fun createDispatcher(
            dispatcherType: DispatcherType,
            rateLimitRequestPerSecond: Int = 500,
            window: DynamicProperty<Int> = DynamicProperty.of(0),
            closingTimeout: Int = 5000,
            profiler: Profiler = NoopProfiler()): RateLimitedDispatcher {
        return when(dispatcherType) {
            DispatcherType.SIMPLE -> RateLimitedDispatcher(
                DISPATCHER_NAME,
                ConfigurableRateLimiter("rate-limiter-name", rateLimitRequestPerSecond),
                profiler,
                window,
                DynamicProperty.of(closingTimeout.toLong())
            )
            DispatcherType.PROVIDER -> {
                RateLimitedDispatcherProviderImpl(
                    rateLimiterFactory = RateLimiterTestFactory(),
                    rateLimiterSettings = DynamicProperty.of(
                        RateLimiterSettings(
                            limitPerSec = rateLimitRequestPerSecond.toDouble(),
                            closeTimeout = Duration.ofSeconds(closingTimeout.toLong()),
                            isSuspendable = false
                        )
                    ),
                    profiler = profiler,
                    suspendableWaitingScopeContext = Dispatchers.Default,
                    limiterId = DISPATCHER_NAME,
                    window = window
                ).provideDispatcher() as RateLimitedDispatcher
            }
        }

    }


    inner class TrackableDispatcher(
            dispatcherType: DispatcherType,
            profiler: Profiler = NoopProfiler()
    ) {

        val windowProperty = AtomicProperty(0)
        private val dispatcher = createDispatcher(profiler = profiler, window = windowProperty, dispatcherType = dispatcherType)
        private val submittedTasksResults = HashMap<Int, CompletableFuture<Any?>>()
        private val isSubmittedTaskInvoked = HashMap<Int, AtomicBoolean>()

        fun submitCompletedTasks(tasks: IntRange) {
            for (task in tasks) {
                submitCompletedTask(task)
            }
        }

        fun submitTasks(tasks: IntRange) {
            for (task in tasks) {
                submitTask(task)
            }
        }

        fun submitCompletedTask(taskIndex: Int) = submitTask(taskIndex, completedFuture(taskIndex))

        fun submitTask(taskIndex: Int) = submitTask(taskIndex, CompletableFuture())

        private fun submitTask(taskIndex: Int, future: CompletableFuture<Any?>) {
            submittedTasksResults[taskIndex] = future
            isSubmittedTaskInvoked[taskIndex] = AtomicBoolean(false)

            dispatcher.compose {
                isSubmittedTaskInvoked[taskIndex]!!.set(true)
                future
            }
        }

        fun completeTask(taskIndex: Int) {
            submittedTasksResults[taskIndex]!!.complete(taskIndex)
        }

        fun completeTasks(range: IntRange) {
            for (task in range) {
                submittedTasksResults[task]!!.complete(task)
            }
        }

        fun isSubmittedTaskInvoked(index: Int) = isSubmittedTaskInvoked[index]!!.get()
        fun isSubmittedTaskInvoked(range: IntRange) = range.all { isSubmittedTaskInvoked[it]!!.get() }

        fun completeAllAndClose() {
            submittedTasksResults.forEach { (_, future) -> future.complete(true) }

            await().atMost(Duration.ofSeconds(10)).until {
                isSubmittedTaskInvoked.all { it.value.get() }
            }
            dispatcher.close()
        }

    }

    @ParameterizedTest
    @EnumSource(DispatcherType::class)
    fun `invoke suspend function wrapped to completable future`(dispatcherType: DispatcherType) {
        val dispatcher = createDispatcher(dispatcherType)

        val suspendFunctionInvoked = AtomicBoolean(false)

        suspend fun mySuspendFunction(): String{
            delay(1000)
            suspendFunctionInvoked.set(true)
            return "suspend-function-result"
        }

        runBlocking {

            val result = dispatcher.compose { future { mySuspendFunction() } }.await()

            suspendFunctionInvoked.get().shouldBeTrue()
            result.shouldBe("suspend-function-result")

        }
    }
}
