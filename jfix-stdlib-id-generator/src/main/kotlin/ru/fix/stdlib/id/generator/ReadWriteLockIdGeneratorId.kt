package ru.fix.stdlib.id.generator

import java.time.Clock
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

class ReadWriteLockIdGeneratorId(
        private val bitsConfiguration: BitsConfiguration,
        private val startOfTime: Long,
        serverId: Long,
        private val clock: Clock,
        private val counter: AtomicLong = AtomicLong(0)
) : IdGenerator {

    private val serverIdPart: Long = serverId and bitsConfiguration.serverPartMaxNumber
    private val idTime = AtomicLong()
    private val lock = ReentrantReadWriteLock()

    private fun generateId(counterValue: Long): Long {
        val tsPart = ((safeTime() - startOfTime) and bitsConfiguration.timePartMaxNumber) shl bitsConfiguration.serverPartBits + bitsConfiguration.counterPartBits
        val counterPart = (counterValue and bitsConfiguration.counterPartMaxNumber) shl bitsConfiguration.serverPartBits

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
     * ts - [BitsConfiguration.timePartBits]
     * counter - [BitsConfiguration.counterPartBits]
     * server_id - [BitsConfiguration.serverPartBits]
     *
     * @return - generated number
     */
    override fun nextId(): Long {

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

    private fun counterIsOverflow(counterValue: Long): Boolean {
        return counterValue > bitsConfiguration.counterPartMaxNumber
    }
}

