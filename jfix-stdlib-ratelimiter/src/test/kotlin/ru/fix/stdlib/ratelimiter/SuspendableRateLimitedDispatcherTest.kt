package ru.fix.stdlib.ratelimiter

import io.kotest.assertions.assertSoftly
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.throwables.shouldThrowExactly
import io.kotest.matchers.booleans.shouldBeFalse
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.collections.shouldContain
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.doubles.shouldBeBetween
import io.kotest.matchers.longs.shouldBeInRange
import io.kotest.matchers.nulls.shouldNotBeNull
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.throwable.shouldHaveCause
import io.kotest.matchers.throwable.shouldHaveCauseOfType
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import mu.KLogging
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Timeout
import ru.fix.aggregating.profiler.*
import ru.fix.dynamic.property.api.DynamicProperty
import java.lang.Thread.sleep
import java.time.Duration
import java.util.concurrent.*
import java.util.concurrent.CompletableFuture.completedFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class SuspendableRateLimitedDispatcherTest {
    private companion object : KLogging() {
        const val DISPATCHER_NAME = "dispatcher-name"
        const val DISPATCHER_METRICS_PREFIX = "RateLimiterDispatcher.$DISPATCHER_NAME"
    }

    val scope: CoroutineScope = CoroutineScope(Dispatchers.Default)

    @Test
    fun `dispatch async operation with successfull CompletableFuture, operation invoked and it's result returned`() {
        val dispatcher = createDispatcher()

        val operationResult = Object()
        fun userAsyncOperation(): CompletableFuture<Any> {
            return completedFuture(operationResult)
        }

        val future = dispatcher.compose { userAsyncOperation() }

        (future.get() === operationResult).shouldBeTrue()
        future.isDone.shouldBeTrue()

        dispatcher.close()
    }

    @Test
    fun `dispatch async operation with successfull result, operation invoked and it's result returned`() = runBlocking {
        val dispatcher = createDispatcher()

        val operationResult = Object()
        fun userAsyncOperation(): Any {
            return operationResult
        }

        val result = dispatcher.decorateSuspend { userAsyncOperation() }

        (result === operationResult).shouldBeTrue()

        dispatcher.close()
    }

    @Test
    fun `dispatch async operation with exceptional CompletableFuture, operation invoked and it's result returned`() {
        val dispatcher = createDispatcher()

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
        actualException.message should {
            it.shouldNotBeNull()
            it.shouldContain(asyncOperationException.message!!)
        }

        dispatcher.close()
    }

    @Test
    fun `dispatch async operation with exceptional result, operation invoked and it's result returned`() = runBlocking {
        val dispatcher = createDispatcher()

        val asyncOperationException = Exception("some error")

        fun userAsyncOperation(): Any {
            throw asyncOperationException
        }

        val actualException = shouldThrow<Exception> { dispatcher.decorateSuspend { userAsyncOperation() } }

        actualException.shouldHaveCause()
        actualException.message should {
            it.shouldNotBeNull()
            it.shouldContain(asyncOperationException.message!!)
        }

        dispatcher.close()
    }

    @Test
    fun `series of operations with CompletableFuture test`() {
        val RATE_PER_SECOND = 500
        val ITERATIONS = 5 * RATE_PER_SECOND

        val report = `submit series of operations with CF`(
                ratePerSecond = RATE_PER_SECOND,
                iterations = ITERATIONS
        )

        val operationReport = report.profilerCallReports.single { it.identity.name == "operation" }

        logger.info("Throughput $operationReport")
        operationReport.stopThroughputAvg.shouldBeBetween(
                RATE_PER_SECOND.toDouble(),
                RATE_PER_SECOND.toDouble(),
                RATE_PER_SECOND.toDouble() * 0.25)
    }

    private fun `submit series of operations with CF`(
            ratePerSecond: Int,
            iterations: Int
    ): ProfilerReport {

        val profiler = AggregatingProfiler()

        val dispatcher = createDispatcher(
                rateLimitRequestPerSecond = ratePerSecond,
                profiler = profiler,
                context = Dispatchers.Unconfined
        )

        val counter = AtomicInteger(0)

        val profilerReporter = profiler.createReporter()
        val profiledCall = profiler.profiledCall("operation")

        val futures = List(iterations) {
                dispatcher.compose {
                    profiledCall.profile<CompletableFuture<Int>> {
                        completedFuture(counter.incrementAndGet())
                    }
                }
            }

        CompletableFuture.allOf(*futures.toTypedArray()).join()

        counter.get().shouldBe(iterations)
        futures.map { it.join() }.toSet().shouldContainAll((1..iterations).toList())

        val report = profilerReporter.buildReportAndReset()
        dispatcher.close()

        return report
    }

    @Test
    fun `series of operations test`() = runBlocking {
        val RATE_PER_SECOND = 500
        val ITERATIONS = 5 * RATE_PER_SECOND

        val report = `submit series of operations`(
                ratePerSecond = RATE_PER_SECOND,
                iterations = ITERATIONS
        )

        val operationReport = report.profilerCallReports.single { it.identity.name == "operation" }

        logger.info("Throughput $operationReport")
        operationReport.stopThroughputAvg.shouldBeBetween(
                RATE_PER_SECOND.toDouble(),
                RATE_PER_SECOND.toDouble(),
                RATE_PER_SECOND.toDouble() * 0.25)
    }

    private suspend fun `submit series of operations`(
            ratePerSecond: Int,
            iterations: Int
    ): ProfilerReport {

        val profiler = AggregatingProfiler()

        val dispatcher = createDispatcher(
                rateLimitRequestPerSecond = ratePerSecond,
                profiler = profiler
        )

        val counter = AtomicInteger(0)

        val profilerReporter = profiler.createReporter()
        val profiledCall = profiler.profiledCall("operation")

        val results = List(iterations) {
            dispatcher.decorateSuspend {
                profiledCall.profile<Int> {
                    counter.incrementAndGet()
                }
            }
        }

        counter.get().shouldBe(iterations)
        results.toSet().containsAll((1..iterations).toList())

        val report = profilerReporter.buildReportAndReset()
        dispatcher.close()

        return report
    }

    @Test
    fun `'acquire_limit', 'supply_operation', 'active_async_operations' metrics gathered during execution with CompletableFuture`() {

        val RATE_PER_SECOND = 500
        val ITERATIONS = 5 * RATE_PER_SECOND

        val report = `submit series of operations with CF`(
                ratePerSecond = RATE_PER_SECOND,
                iterations = ITERATIONS
        )

        report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.acquire_limit" }
                .stopSum.shouldBe(ITERATIONS)

        report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.supply_operation" }
                .stopSum.shouldBe(ITERATIONS)

        report.indicators.map { it.key.name }.shouldContain("$DISPATCHER_METRICS_PREFIX.active_async_operations")

        logger.info { "Resulting profiler report: $report" }
    }

    @Test
    fun `'acquire_limit', 'supply_operation', 'active_async_operations' metrics gathered during execution`() = runBlocking {

        val RATE_PER_SECOND = 500
        val ITERATIONS = 5 * RATE_PER_SECOND

        val report = `submit series of operations`(
                ratePerSecond = RATE_PER_SECOND,
                iterations = ITERATIONS
        )

        report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.acquire_limit" }
                .stopSum.shouldBe(ITERATIONS)

        report.profilerCallReports.single { it.identity.name == "$DISPATCHER_METRICS_PREFIX.supply_operation" }
                .stopSum.shouldBe(ITERATIONS)

        report.indicators.map { it.key.name }.shouldContain("$DISPATCHER_METRICS_PREFIX.active_async_operations")

        logger.info { "Resulting profiler report: $report" }
    }

    @Test
    fun `indicators 'active_async_operations' adjusted according to number of queued and active operations with CompletableFuture`() {
        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()
        val trackableDispatcherWithCF = TrackableDispatcherWithCF(profiler)

        trackableDispatcherWithCF.submitTasks(1..12, sleepTo = 1000)
        await().atMost(Duration.ofSeconds(2)).until {
            trackableDispatcherWithCF.isSubmittedTaskInvoked(1..10)
        }

        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 12
        }

        await().atMost(Duration.ofSeconds(1)).until {
            trackableDispatcherWithCF.isSubmittedTaskFinished(1..10)
        }
        await().atMost(Duration.ofSeconds(3)).until {
            trackableDispatcherWithCF.isSubmittedTaskInvoked(11..12)
        }

        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBeInRange LongRange(0, 2)
        }

        await().atMost(Duration.ofSeconds(1)).until {
            trackableDispatcherWithCF.isSubmittedTaskFinished(1..12)
        }
        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 0
        }

        trackableDispatcherWithCF.completeAllAndClose()
    }

    @Test
    fun `indicators 'active_async_operations' adjusted according to number of queued and active operations`() {
        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()
        val trackableDispatcher = TrackableDispatcher(profiler)

        trackableDispatcher.submitTasks(1..12)
        await().atMost(Duration.ofSeconds(2)).until {
            trackableDispatcher.isSubmittedTaskInvoked(1..10)
        }

        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 12
        }

        await().atMost(Duration.ofSeconds(1)).until {
            trackableDispatcher.isSubmittedTaskFinished(1..10)
        }
        await().atMost(Duration.ofSeconds(3)).until {
            trackableDispatcher.isSubmittedTaskInvoked(11..12)
        }

        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBeInRange LongRange(0, 2)
        }

        await().atMost(Duration.ofSeconds(1)).until {
            trackableDispatcher.isSubmittedTaskFinished(1..12)
        }
        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 0
        }

        trackableDispatcher.completeAllAndClose()
    }

    @Test
    fun `WHEN many fast CompletableFuture tasks completed THEN 'active_async_operations' is 0`() {
        val report = `submit series of operations with CF`(500, 4000)

        logger.info { report }
        report.assertSoftly {
            it.getMetric("active_async_operations") shouldBe 0
        }
        return
    }

    @Test
    fun `WHEN many fast tasks completed THEN 'active_async_operations' is 0`() = runBlocking {
        val report = `submit series of operations`(500, 4000)

        logger.info { report }
        report.assertSoftly {
            it.getMetric("active_async_operations") shouldBe 0
        }
        return@runBlocking
    }

    @Test
    fun `WHEN completed futures arrived THEN indicators are correctly adjusted`() {
        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()
        val trackableDispatcherWithCF = TrackableDispatcherWithCF(profiler)

        trackableDispatcherWithCF.submitCompletedTasks(1..4)
        trackableDispatcherWithCF.submitTasks(5..6, 3000) // long executed tasks
        await().atMost(Duration.ofSeconds(1)).until {
            trackableDispatcherWithCF.isSubmittedTaskInvoked(1..6)
        }
        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 2
        }

        trackableDispatcherWithCF.submitCompletedTasks(7..10)
        await().atMost(Duration.ofSeconds(1)).until {
            trackableDispatcherWithCF.isSubmittedTaskInvoked(7..10)
        }
        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 2
        }

        await().atMost(Duration.ofSeconds(3)).until {
            trackableDispatcherWithCF.isSubmittedTaskFinished(1..10)
        }
        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 0
        }

        trackableDispatcherWithCF.completeAllAndClose()
    }

    @Test
    fun `WHEN results arrived THEN indicators are correctly adjusted`() {
        val profiler = AggregatingProfiler()
        val reporter = profiler.createReporter()
        val trackableDispatcher = TrackableDispatcher(profiler)

        trackableDispatcher.submitCompletedTasks(1..4)
        trackableDispatcher.submitTasks(5..6, 3000) // long executed tasks
        await().atMost(Duration.ofSeconds(1)).until {
            trackableDispatcher.isSubmittedTaskInvoked(1..6)
        }
        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 2
        }

        trackableDispatcher.submitCompletedTasks(7..10)
        await().atMost(Duration.ofSeconds(1)).until {
            trackableDispatcher.isSubmittedTaskInvoked(7..10)
        }
        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 2
        }

        await().atMost(Duration.ofSeconds(3)).until {
            trackableDispatcher.isSubmittedTaskFinished(1..10)
        }
        reporter.buildReportAndReset().assertSoftly {
            it.getMetric("active_async_operations") shouldBe 0
        }

        trackableDispatcher.completeAllAndClose()
    }

    /**
     * When dispatcher closingTimeout is enough for pending tasks to complete
     * such tasks will complete normally
     */
    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `on shutdown fast tasks with CompletableFuture complete normally`() {

        val dispatch = createDispatcher(closingTimeout = 5_000)

        val futures = List(3) {
            dispatch.compose {
                completedFuture(true)
            }
        }
        dispatch.close()

        futures.forEach { future: CompletableFuture<*> ->
            future.isDone.shouldBeTrue()
            future.isCompletedExceptionally.shouldBeFalse()
        }
    }

    /**
     * When dispatcher closingTimeout is enough for pending tasks to complete
     * such tasks will complete normally
     */
    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `on shutdown fast tasks complete normally`() = runBlocking {

        val dispatch = createDispatcher(closingTimeout = 5_000)

        val results = List(3) {
            dispatch.decorateSuspend {
                true
            }
        }
        dispatch.close()

        results.forEach { result ->
            result.shouldBeTrue()
        }
    }

    @Test
    @Timeout(5, unit = TimeUnit.SECONDS)
    fun `on shutdown slow tasks with CompletableFuture complete exceptionally`() {
        val dispatch = createDispatcher(closingTimeout = 0)

        for (i in 1..3) {
            scope.launch {
                dispatch.compose {
                    GlobalScope.future {
                        delay(3_000)
                        true
                    }
                }
            }
        }

        val exc = shouldThrow<RejectedExecutionException> {
            dispatch.close()
        }
        exc.message!!.contains("timeout while waiting for coroutines finishing").shouldBeTrue()
    }

    @Test
    @Timeout(5, unit = TimeUnit.SECONDS)
    fun `on shutdown slow tasks complete exceptionally`() {
        val dispatch = createDispatcher(closingTimeout = 0)

        for (i in 1..3) {
            scope.launch {
                dispatch.decorateSuspend {
                    delay(3_000)
                }
            }
        }

        val exc = shouldThrow<RejectedExecutionException> {
            dispatch.close()
        }
        exc.message!!.contains("timeout while waiting for coroutines finishing").shouldBeTrue()
    }

    @Test
    fun `task with CompletableFuture, submitted in closed dispatcher, is rejected with exception`() {
        val dispatcher = createDispatcher()
        dispatcher.close()

        val t = shouldThrowExactly<CompletionException> {
            dispatcher.compose {
                completedFuture(true)
            }.join()
        }
        t.shouldHaveCauseOfType<RejectedExecutionException>()
        t.message should {
            it.shouldNotBeNull()
            it.shouldContain("TERMINATE")
        }
        return
    }

    @Test
    fun `task, submitted in closed dispatcher, is rejected with exception`() = runBlocking {
        val dispatcher = createDispatcher()
        dispatcher.close()

        val t = shouldThrowExactly<RejectedExecutionException> {
            dispatcher.decorateSuspend {
                true
            }
        }
        t.message should {
            it.shouldNotBeNull()
            it.shouldContain("TERMINATE")
        }
        return@runBlocking
    }

    @Test
    fun `invoke suspend function wrapped to completable future`() {
        val dispatcher = createDispatcher()

        val suspendFunctionInvoked = AtomicBoolean(false)

        suspend fun mySuspendFunction(): String{
            delay(500)
            suspendFunctionInvoked.set(true)
            return "suspend-function-result"
        }

        runBlocking {
            val result = dispatcher.compose { future { mySuspendFunction() } }.await()

            suspendFunctionInvoked.get().shouldBeTrue()
            result.shouldBe("suspend-function-result")
        }
    }

    @Test
    fun `invoke suspend function`() {
        val dispatcher = createDispatcher()

        val suspendFunctionInvoked = AtomicBoolean(false)

        suspend fun mySuspendFunction(): String{
            delay(500)
            suspendFunctionInvoked.set(true)
            return "suspend-function-result"
        }

        runBlocking {
            val result = dispatcher.decorateSuspend { mySuspendFunction() }

            suspendFunctionInvoked.get().shouldBeTrue()
            result.shouldBe("suspend-function-result")
        }
    }

    fun createDispatcher(
            rateLimitRequestPerSecond: Int = 500,
            closingTimeout: Int = 5000,
            profiler: Profiler = NoopProfiler(),
            context: CoroutineContext = Dispatchers.IO
    ) =
            SuspendableRateLimitedDispatcher(
                    DISPATCHER_NAME,
                    ConfigurableRateLimiterKt("rate-limiter-name", rateLimitRequestPerSecond),
                    profiler,
                    DynamicProperty.of(closingTimeout.toLong()),
                    context
            )

    inner class TrackableDispatcherWithCF(
            profiler: Profiler = NoopProfiler()
    ) {

        private val dispatcher = createDispatcher(profiler = profiler)
        val isSubmittedTaskInvoked = HashMap<Int, AtomicBoolean>()
        val isSubmittedTaskFinished = HashMap<Int, AtomicBoolean>()

        fun submitCompletedTasks(tasks: IntRange) {
            for (task in tasks) {
                submitCompletedTask(task)
            }
        }

        fun submitTasks(tasks: IntRange, sleepTo: Long) {
            for (task in tasks) {
                submitTask(task, sleepTo)
            }
        }

        private fun submitCompletedTask(taskIndex: Int) = submitTask(taskIndex, completedFuture(taskIndex), 0)

        private fun submitTask(taskIndex: Int, sleepTo: Long) = submitTask(taskIndex, CompletableFuture(), sleepTo)

        private fun submitTask(taskIndex: Int, future: CompletableFuture<Any?>, sleepTo: Long) {
            isSubmittedTaskInvoked[taskIndex] = AtomicBoolean(false)
            isSubmittedTaskFinished[taskIndex] = AtomicBoolean(false)

            dispatcher.compose {
                logger.info { "Setting isSubmittedTaskInvoked with index $taskIndex to true" }
                isSubmittedTaskInvoked[taskIndex]!!.set(true)
                if (sleepTo > 0) {
                    sleep(sleepTo)    //long operation imitation with blocking
                    future.complete(taskIndex)
                }
                logger.info { "Setting isSubmittedTaskFinished with index $taskIndex to true" }
                isSubmittedTaskFinished[taskIndex]!!.set(true)
                logger.info { "Index has been set for index $taskIndex" }
                future
            }

        }

        fun isSubmittedTaskInvoked(range: IntRange) = range.all { isSubmittedTaskInvoked[it]!!.get() }
        fun isSubmittedTaskFinished(range: IntRange) = range.all { isSubmittedTaskFinished[it]!!.get() }

        fun completeAllAndClose() {
            await().atMost(Duration.ofSeconds(10)).until {
                isSubmittedTaskInvoked.all { it.value.get() }
            }
            dispatcher.close()
        }

    }

    inner class TrackableDispatcher(
            profiler: Profiler = NoopProfiler()
    ) {

        private val dispatcher = createDispatcher(profiler = profiler)
        val isSubmittedTaskInvoked = HashMap<Int, AtomicBoolean>()
        val isSubmittedTaskFinished = HashMap<Int, AtomicBoolean>()

        fun submitCompletedTasks(tasks: IntRange) {
            for (task in tasks) {
                submitCompletedTask(task)
            }
        }

        fun submitTasks(tasks: IntRange, sleepTo: Long = 1000) {
            for (task in tasks) {
                submitTask(task, sleepTo)
            }
        }

        private fun submitCompletedTask(taskIndex: Int) = submitTask(taskIndex, 0)

        private fun submitTask(taskIndex: Int, sleepTo: Long) {
            isSubmittedTaskInvoked[taskIndex] = AtomicBoolean(false)
            isSubmittedTaskFinished[taskIndex] = AtomicBoolean(false)

            scope.launch {
                dispatcher.decorateSuspend {
                    logger.info { "Setting isSubmittedTaskInvoked with index $taskIndex to true" }
                    isSubmittedTaskInvoked[taskIndex]!!.set(true)
                    if (sleepTo > 0) {
                        sleep(sleepTo)    //long operation imitation with blocking
                    }
                    logger.info { "Setting isSubmittedTaskFinished with index $taskIndex to true" }
                    isSubmittedTaskFinished[taskIndex]!!.set(true)
                }
            }

        }

        fun isSubmittedTaskInvoked(range: IntRange) = range.all { isSubmittedTaskInvoked[it]!!.get() }
        fun isSubmittedTaskFinished(range: IntRange) = range.all { isSubmittedTaskFinished[it]!!.get() }

        fun completeAllAndClose() {
            await().atMost(Duration.ofSeconds(10)).until {
                isSubmittedTaskInvoked.all { it.value.get() }
            }
            dispatcher.close()
        }

    }

    private fun ProfilerReport.getMetric(metric: String): Long {
        return indicators.mapKeys { it.key.name }["$DISPATCHER_METRICS_PREFIX.$metric"]!!
    }

}