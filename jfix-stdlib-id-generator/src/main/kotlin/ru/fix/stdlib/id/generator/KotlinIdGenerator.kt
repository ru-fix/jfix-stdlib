package ru.fix.stdlib.id.generator

import java.time.Clock
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

class KotlinIdGenerator {

    private val startOfTime: Long

    private val clock: Clock

    private val counter: AtomicLong

    private val serverIdPart: Long

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

    private fun generateId(time: Long, counterValue: Long): Long {
        val now = clock.millis()
        //4
        if (time < now) {
            idTime.set(now)
        }

        // TODO: make pretty
        val tsPart = ((idTime.get() - startOfTime) and byteConfiguration.timePartMaxNumber) shl byteConfiguration.serverPartBytes + byteConfiguration.counterPartBytes
        val counterPart = (counterValue and byteConfiguration.counterPartMaxNumber) shl byteConfiguration.serverPartBytes

        return tsPart or counterPart or serverIdPart
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

            var time = idTime.get()
            var counterValue = counter.incrementAndGet()

            // 1
            if (isCounterOverflow(counterValue)) {

                readLock.unlock()
                writeLock.lock()

                try {
                    //2
                    counterValue = counter.get()
                    if (isCounterOverflow(counterValue)) {
                        time = resetOverflowAndGetSafeTime()
                        id = generateId(time, counterValue)
                    } else {
                        counterValue = counter.incrementAndGet()
                        //3
                        if (isCounterOverflow(counterValue)) {
                            time = resetOverflowAndGetSafeTime()
                            id = generateId(time, counterValue)
                        } else {
                            id = generateId(time, counterValue)
                        }
                    }
                    readLock.lock()
                } finally {
                    writeLock.unlock()
                }

            } else {
                id = generateId(time, counterValue)
            }

        } finally {
            readLock.unlock()
        }

        return id
    }

    private fun resetOverflowAndGetSafeTime(): Long {
        counter.set(0)
        return idTime.incrementAndGet()
    }

    private fun isCounterOverflow(counterValue: Long): Boolean {
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

        //TODO: проверка что значения не могут быть отрицательными

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