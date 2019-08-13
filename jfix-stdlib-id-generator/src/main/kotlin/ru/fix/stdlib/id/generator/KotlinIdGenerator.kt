package ru.fix.stdlib.id.generator

import java.time.Clock
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

class KotlinIdGenerator {

    private val startOfTime: Long
    private val clock: Clock
    private val serverIdPart: Long
    private val counter: AtomicLong
    private val idTime = AtomicLong()
    private val byteConfiguration: ByteConfiguration
    private val lock = ReentrantReadWriteLock()

    constructor(byteConfiguration: ByteConfiguration, startOfTime: Long, serverId: Long, clock: Clock, counter: AtomicLong) {
        this.byteConfiguration = byteConfiguration
        this.startOfTime = startOfTime;
        this.serverIdPart = serverId and byteConfiguration.serverPartMaxNumber
        this.clock = clock
        this.counter = counter
    }

    private fun generateId(counterValue: Long): Long {
        val tsPart = ((safeTime() - startOfTime) and byteConfiguration.timePartMaxNumber) shl byteConfiguration.serverPartBytes + byteConfiguration.counterPartBytes
        val counterPart = (counterValue and byteConfiguration.counterPartMaxNumber) shl byteConfiguration.serverPartBytes

        return tsPart or counterPart or serverIdPart
    }

    private fun resetCounterOverflowAndGenerateId(): Long {
        counter.set(0)
        idTime.incrementAndGet()
        return generateId(counter.get())
    }

    private fun safeTime(): Long {
        val now = clock.millis()
        //4
        if (idTime.get() < now) {
            idTime.set(now)
        }

        return idTime.get()
    }

    /**
     * Generate unique incremental number in the following order: [ ts | counter | server ] where:
     * ts - [ByteConfiguration.timePartBytes]
     * counter - [ByteConfiguration.counterPartBytes]
     * server_id - [ByteConfiguration.serverPartBytes]
     *
     * @return - generated number
     */
    fun nextId(): Long {

        val readLock = lock.readLock()
        val writeLock = lock.writeLock()

        val id: Long

        readLock.lock()
        try {

            var counterValue = counter.incrementAndGet()

            // 1
            if (counterIsOverflow(counterValue)) {

                readLock.unlock()
                writeLock.lock()

                try {
                    //2
                    counterValue = counter.get()
                    if (counterIsOverflow(counterValue)) {
                        id = resetCounterOverflowAndGenerateId()
                    } else {
                        counterValue = counter.incrementAndGet()
                        //3
                        @Suppress("LiftReturnOrAssignment")
                        if (counterIsOverflow(counterValue)) {
                            id = resetCounterOverflowAndGenerateId()
                        } else {
                            id = generateId(counterValue)
                        }
                    }
                    readLock.lock()
                } finally {
                    writeLock.unlock()
                }

            } else {
                id = generateId(counterValue)
            }

        } finally {
            readLock.unlock()
        }

        return id
    }

    @Synchronized
    fun nextId2(): Long {
        var currentValue = counter.incrementAndGet()

        return if (currentValue > byteConfiguration.counterPartMaxNumber) {
            idTime.incrementAndGet()
            counter.set(0)
            currentValue = 0

            generateId(currentValue)
        } else {
            generateId(currentValue)
        }
    }

    private fun counterIsOverflow(counterValue: Long): Boolean {
        return counterValue > byteConfiguration.counterPartMaxNumber
    }
}

class ByteConfiguration(
        val serverPartBytes: Int,
        val timePartBytes: Int,
        val counterPartBytes: Int
) {

    val serverPartMaxNumber: Long
    val timePartMaxNumber: Long
    val counterPartMaxNumber: Long

    init {

        val numberOfBytes = serverPartBytes + timePartBytes + counterPartBytes

        check(numberOfBytes <= 64) {
            "All parts must be no more than 64 bytes, real $numberOfBytes"
        }

        check(serverPartBytes > 0 && timePartBytes > 0 && counterPartBytes > 0) {
            """Number of bytes must be greater than 0, 
                |serverPartBytes=$serverPartBytes, 
                |timePartBytes=$timePartBytes, 
                |counterPartBytes=$counterPartBytes""".trimMargin()
        }

        serverPartMaxNumber = maxNumberForBytes(serverPartBytes)
        timePartMaxNumber = maxNumberForBytes(timePartBytes)
        counterPartMaxNumber = maxNumberForBytes(counterPartBytes)

    }

    private fun maxNumberForBytes(numberOfBytes: Int): Long {

        var binaryNumber = ""
        for (number in 1..numberOfBytes) {
            binaryNumber += "1"
        }

        return binaryNumber.toLong(2)
    }
}