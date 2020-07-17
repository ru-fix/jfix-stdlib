package ru.fix.stdlib.ratelimiter

import io.kotest.matchers.booleans.shouldBeTrue
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.lessThanOrEqualTo
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import ru.fix.aggregating.profiler.AggregatingProfiler
import ru.fix.aggregating.profiler.NoopProfiler
import ru.fix.aggregating.profiler.ProfiledCall
import ru.fix.dynamic.property.api.DynamicProperty
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

class RateLimitedDispatcherTest {

    @Test
    fun `submit async operation with user defined async result`(){

        class UserAsyncResult(){
            fun whenComplete(callback: ()->Unit){

            }
        }

        val dispatcher = RateLimitedDispatcher(
                "dispatcher-name",
                 ConfigurableRateLimiter("rate-limiter-name", 100),
                NoopProfiler(),
                DynamicProperty.of(5_000)
        )

        val USER_ASYNC_RESULT = UserAsyncResult()
        fun userAsyncOperation(): UserAsyncResult {
            return USER_ASYNC_RESULT
        }

        val deleyedSubmission = dispatcher.submit(
                { userAsyncOperation() },
                { asyncResult, callback -> asyncResult.whenComplete( { callback()} ) }
        )

        (USER_ASYNC_RESULT === deleyedSubmission.get()).shouldBeTrue()

    }

    @Test
    fun `'queue_size' indicator shows unprocessed pending requests in queue`(){

    }

    @Test
    fun `'queue_wait' metric shows time span between enqueueing and dequeueing`(){

    }

    @Test
    fun `'acquire_limit' metric shows how long it took to pass rate limit restriction`(){

    }

    @Test
    fun `'acquire_window' metric shows how long it took to pass window size restriction`(){

    }


    @Test
    fun `metric shows queue-wait`(){

    }


    @Test
    fun `submit async operation with CompletableFuture result`(){

    }




    @Test
    fun `if windows size is empty, then restricted only by limiter `(){
    }

    @Test
    fun `if window size is not empty, still restricted by limiter`(){
    }

    @Test
    fun `window blocks number of uncompleted operations `(){
    }








    @Test
    fun testSubmitIncrementThroughput() {
        createDispatcher(2000).use {
            assertTimeoutPreemptively(Duration.ofSeconds(15)) {
                testThroughput { call, counter -> submitIncrement(it, call, counter) }
            }
        }
    }

    @Test
    fun testComposeIncrementThroughput() {
        createDispatcher(2000).use {
            assertTimeoutPreemptively(Duration.ofSeconds(15)) {
                testThroughput { call, counter -> composeIncrement(it, call, counter) }
            }
        }
    }

    /**
     * When dispatcher closingTimeout is anought for pending tasks to complete
     * such tasks will complete normally
     */
    @Test
    fun shutdown_tasksCompletedInTimeout_areCompletedNormally() {

        createDispatcher(5_000).use { dispatch ->
            assertTimeoutPreemptively(Duration.ofSeconds(10)) {

                val blockingTaskIsStarted = CountDownLatch(1)


                dispatch.submit {
                    blockingTaskIsStarted.countDown()
                    //Due to blocking nature of dispatch.close we hae to use sleep
                    Thread.sleep(1000)
                }
                val futures = List(3) {
                    dispatch.submit { }
                }

                blockingTaskIsStarted.await()
                dispatch.close()

                CompletableFuture.allOf(*futures.toTypedArray()).exceptionally { null }.join()

                futures.forEach { future: CompletableFuture<*> ->
                    assertTrue(future.isDone)
                    assertFalse(future.isCompletedExceptionally)
                }
            }
        }
    }

    @Test
    fun shutdown_tasksNotCompletedInTimeout_areCompletedExceptionally() {
        createDispatcher(0).use { dispatch ->
            assertTimeoutPreemptively(Duration.ofSeconds(5)) {
                val blockingTaskIsStarted = CountDownLatch(1)

                dispatch.submit {
                    blockingTaskIsStarted.countDown()
                    //Due to blocking nature of dispatch.close we hae to use sleep
                    Thread.sleep(1000)
                }

                val futures = ArrayList<CompletableFuture<*>>()
                for (i in 1..3) {
                    futures.add(dispatch.submit { })
                }

                blockingTaskIsStarted.await()
                dispatch.close()

                CompletableFuture.allOf(*futures.toTypedArray()).exceptionally { null }.join()

                futures.forEach { future: CompletableFuture<*> ->
                    assertTrue(future.isDone)
                    assertTrue(future.isCompletedExceptionally)
                }
            }
        }
    }

    private fun createDispatcher(closingTimeout: Long): RateLimitedDispatcher {
        val limiter = ConfigurableRateLimiter("test-rate-limiter", RATE_LIMIT)
        return RateLimitedDispatcher("test-rate-limiter-dispatcher", limiter, NoopProfiler(),
                DynamicProperty.of(closingTimeout))
    }


    private fun testThroughput(biFunction: (ProfiledCall, AtomicInteger) -> CompletableFuture<Int>) {
        val counter = AtomicInteger(0)
        val profiler = AggregatingProfiler()
        val profilerReporter = profiler.createReporter()

        profilerReporter.buildReportAndReset()
        val profiledCall = profiler.profiledCall(RateLimitedDispatcher::class.toString())

        val features = List(ITERATIONS) {
            biFunction.invoke(profiledCall, counter)
        }

        CompletableFuture.allOf(*features.toTypedArray()).join()
        val report = profilerReporter.buildReportAndReset().profilerCallReports[0]

        val results = features
                .map { it.join() }

        for (i in 0 until ITERATIONS) {
            assertTrue(results.contains(i))
        }

        LOGGER.info("Current throughput " + report.stopThroughputAvg)

        assertThat(report.stopThroughputAvg, lessThanOrEqualTo((RATE_LIMIT * 1.25 * 1000.toDouble())))


        assertEquals(ITERATIONS, counter.get())
    }

    private fun submitIncrement(dispatcher: RateLimitedDispatcher,
                                call: ProfiledCall,
                                counter: AtomicInteger): CompletableFuture<Int> {
        return dispatcher.submit {
            call.profile<Int> {
                counter.getAndIncrement()
            }
        }
    }

    private fun composeIncrement(dispatcher: RateLimitedDispatcher,
                                 call: ProfiledCall,
                                 counter: AtomicInteger): CompletableFuture<Int> {
        return dispatcher.compose {
            call.profile<CompletableFuture<Int>> {
                CompletableFuture.supplyAsync { counter.getAndIncrement() }
            }
        }
    }

    companion object {

        private val LOGGER = LoggerFactory.getLogger(RateLimitedDispatcherTest::class.java)
        private val RATE_LIMIT = 570
        private val ITERATIONS = 5 * RATE_LIMIT
    }

}
