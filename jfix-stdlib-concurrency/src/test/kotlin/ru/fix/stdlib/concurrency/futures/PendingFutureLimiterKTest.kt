package ru.fix.stdlib.concurrency.futures

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertTimeoutPreemptively
import ru.fix.dynamic.property.api.DynamicProperty
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

private val assertTimeoutDuration = Duration.ofSeconds(10)
private val internalLargeTimeoutMs: Long = TimeUnit.MINUTES.toMillis(1)
private const val CHECKS_QUANTITY = 100

internal class PendingFutureLimiterKTest {

    private val limiter = PendingFutureLimiter(
            DynamicProperty.of(2),
            DynamicProperty.of(internalLargeTimeoutMs)
    )


    @Test
    fun `waitAll with timeout WHEN enqueue completed future AND breaks exist THEN limiter doesn't stuck`() {
        var checkCounter = 0
        assertTimeoutPreemptively(assertTimeoutDuration, { "failed on $checkCounter check" }) {
            while (checkCounter < CHECKS_QUANTITY) {
                checkCounter++
                limiter.enqueueBlocking(CompletableFuture.completedFuture(null))
                Thread.sleep(10)
                limiter.waitAll(internalLargeTimeoutMs)
            }
        }
    }

    @Test
    fun `waitAll with timeout WHEN enqueue 2 completed future THEN limiter doesn't stuck`() {
        var checkCounter = 0
        assertTimeoutPreemptively(assertTimeoutDuration, { "failed on $checkCounter check" }) {
            while (checkCounter < CHECKS_QUANTITY) {
                checkCounter++
                limiter.enqueueBlocking(CompletableFuture.completedFuture(null))
                limiter.enqueueBlocking(CompletableFuture.completedFuture(null))
                limiter.waitAll(internalLargeTimeoutMs)
            }
        }
    }

    @Test
    fun `waitAll with timeout WHEN enqueue completed future THEN limiter doesn't stuck`() {
        var checkCounter = 0
        assertTimeoutPreemptively(assertTimeoutDuration, { "failed on $checkCounter check" }) {
            while (checkCounter < CHECKS_QUANTITY) {
                checkCounter++
                limiter.enqueueBlocking(CompletableFuture.completedFuture(null))
                limiter.waitAll(internalLargeTimeoutMs)
            }
        }
    }

    @Test
    fun `waitAll without timeout WHEN enqueue completed future THEN limiter doesn't stuck`() {
        var checkCounter = 0
        assertTimeoutPreemptively(assertTimeoutDuration, { "failed on $checkCounter check" }) {
            while (checkCounter < CHECKS_QUANTITY) {
                checkCounter++
                limiter.enqueueBlocking(CompletableFuture.completedFuture(null))
                limiter.waitAll()
            }
        }
    }

    @Test
    fun `waitAll with timeout WHEN enqueue not completed future THEN limiter doesn't stuck`() {
        val lock = ReentrantLock()
        var checkCounter = 0
        assertTimeoutPreemptively(assertTimeoutDuration, { "failed on $checkCounter check" }) {
            while (checkCounter < CHECKS_QUANTITY) {
                checkCounter++
                lock.lock()
                limiter.enqueueBlocking(CompletableFuture.runAsync { lock.withLock {} })
                lock.unlock() //ensure future wasn't completed before being queued
                limiter.waitAll(internalLargeTimeoutMs)
            }
        }
    }

    @Test
    fun `waitAll without timeout WHEN enqueue not completed future THEN limiter doesn't stuck`() {
        val lock = ReentrantLock()
        var checkCounter = 0
        assertTimeoutPreemptively(assertTimeoutDuration, { "failed on $checkCounter check" }) {
            while (checkCounter < CHECKS_QUANTITY) {
                checkCounter++
                lock.lock()
                limiter.enqueueBlocking(CompletableFuture.runAsync { lock.withLock {} })
                lock.unlock() //ensure future wasn't completed before being queued
                limiter.waitAll()
            }
        }
    }

    @Test
    fun `waitAll with timeout WHEN enqueue unlimited completed future THEN limiter doesn't stuck`() {
        var checkCounter = 0
        assertTimeoutPreemptively(assertTimeoutDuration, { "failed on $checkCounter check" }) {
            while (checkCounter < CHECKS_QUANTITY) {
                checkCounter++
                limiter.enqueueUnlimited(CompletableFuture.completedFuture(null))
                limiter.waitAll()
            }
        }
    }

    @Test
    fun `waitAll with timeout WHEN enqueued slow future THEN limiter doesn't stuck`() {
        assertTimeoutPreemptively(assertTimeoutDuration) {
            limiter.enqueueUnlimited(CompletableFuture.runAsync {
                Thread.sleep(assertTimeoutDuration.toMillis() / 2)
            })
            limiter.waitAll(internalLargeTimeoutMs)
        }
    }
}