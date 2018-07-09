package ru.fix.stdlib.ratelimiter

import mu.KotlinLogging
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.temporal.ChronoUnit


private val log = KotlinLogging.logger { }

class ConfigurableRateLimiterTest {

    @Test
    fun `getRate returns valid rate`() {
        val limitsPerSecond = 353
        val limiter = ConfigurableRateLimiter("test-limiter", limitsPerSecond)

        assertEquals(limitsPerSecond.toDouble(), limiter.rate)
    }

    @Test
    fun `On any 1 second time interval taken in any position in time limit is not exceeded under ragged load`() {

        val DURATION_MS = 10_000
        val SLEEP_AFTER = 5500L
        val SLEEP_DELAY = 2000L
        val RATE_LIMIT = 350


        val limiter = ConfigurableRateLimiter("test-limiter", RATE_LIMIT)

        val accumulator = Accumulator(DURATION_MS)
        val sleeper = Sleeper(SLEEP_AFTER, SLEEP_DELAY)

        log.info {
            """
            Start emitting events with rate: $RATE_LIMIT
            After $SLEEP_AFTER ms pause emitting thread.
            Sleep for $SLEEP_DELAY ms.
            Resume emitting delays.
            Expected test run time: $DURATION_MS ms
            """
        }

        do {
            sleeper.sleepIfRequired()
            limiter.tryAcquire(10, ChronoUnit.SECONDS)
        } while (accumulator.registerEvent())

        log.info { "Events emitting is completed." }

        val report = accumulator.calculateMaxSumWithinOneSecondSlidingWindowAndBuildReport()

        log.info { "Expected rate: $RATE_LIMIT, actual max rate: ${report.maxRate}" }

        log.info { report }

        assertTrue(
                report.maxRate < RATE_LIMIT * 1.05, //to exclude time measurement error
                """
                    |Max event sum on 1 second interval [$report.maxRate] < Expected rate [$RATE_LIMIT]
                    |Events in particular millisecond, rate on 1s interval:
                    |$report
                """.trimMargin()
        )
    }

    /**
     * stores event count in array
     * index of array - is timestamp of event starting with first received event
     */
    class Accumulator(val sizeMs: Int) {

        val events = IntArray(sizeMs)

        var firstEventTimestampMs = 0L

        private val SLIDING_WINDOW_SIZE = 1000 //1 second

        /**
         * @return false is accumulator is full
         */
        fun registerEvent(): Boolean {
            val time = System.currentTimeMillis()

            if (firstEventTimestampMs == 0L) {
                firstEventTimestampMs = time
            }

            if (time >= firstEventTimestampMs + sizeMs) {
                return false
            }

            events[(time - firstEventTimestampMs).toInt()]++
            return true
        }

        data class Report(val maxRate: Int, val report: String)

        fun calculateMaxSumWithinOneSecondSlidingWindowAndBuildReport(): Report {
            var maxSum = 0
            val report = StringBuilder()

            for (i in 0 until SLIDING_WINDOW_SIZE) {
                if (events[i] != 0) {
                    report.appendln("event[$i]=${events[i]}, rate not defined, not enough previous event")
                }
            }

            for (i in SLIDING_WINDOW_SIZE until events.size) {
                val windowSum = (i - SLIDING_WINDOW_SIZE..i)
                        .asIterable()
                        .map { events[it] }
                        .reduce { acc, item -> acc + item }

                if (events[i] != 0) {
                    report.appendln("event[$i]=${events[i]}, rate: $windowSum")
                }
                maxSum = Math.max(maxSum, windowSum)
            }
            return Report(maxSum, report.toString())
        }
    }

    class Sleeper(private val sleepAfterMs: Long, private val sleepTimeMs: Long) {
        var isSlept = false
        var firstCheckTimestampMs = 0L

        fun sleepIfRequired() {

            if (firstCheckTimestampMs == 0L) {
                firstCheckTimestampMs = System.currentTimeMillis()
            }

            if (!isSlept && System.currentTimeMillis() >= firstCheckTimestampMs + sleepAfterMs) {
                isSlept = true
                Thread.sleep(sleepTimeMs)
            }
        }
    }

}