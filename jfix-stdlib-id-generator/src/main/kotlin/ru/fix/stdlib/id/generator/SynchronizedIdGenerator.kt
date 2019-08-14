package ru.fix.stdlib.id.generator

import java.time.Clock

class SynchronizedIdGenerator(
        private val bitsConfiguration: BitsConfiguration,
        private val startOfTime: Long,
        serverId: Long,
        private val clock: Clock,
        counter: Long = 0L
) : IdGenerator {

    private val serverIdPart: Long
    private var counter: Long
    private var idTime: Long = 0L

    init {
        require(serverId <= bitsConfiguration.serverPartMask) {
            "serverId cannot be greater than ${bitsConfiguration.serverPartMask}"
        }

        require(startOfTime < clock.millis()) {
            "startOfTime cannot be greater than $startOfTime"
        }

        this.serverIdPart = serverId and bitsConfiguration.serverPartMask
        this.counter = counter
    }

    @Synchronized
    override fun nextId(): Long {
        counter++
        return if (counterIsOverflow(counter)) {
            idTime++
            counter = 0
            generateId(counter)
        } else {
            generateId(counter)
        }
    }

    private fun generateId(counterValue: Long): Long {
        catchUpIdTimeWithCurrentTimeIfNeeded()
        val tsPart = ((idTime - startOfTime) and bitsConfiguration.timePartMask) shl bitsConfiguration.serverPartBits + bitsConfiguration.counterPartBits
        val counterPart = (counterValue and bitsConfiguration.counterPartMask) shl bitsConfiguration.serverPartBits

        return tsPart or counterPart or serverIdPart
    }

    private fun catchUpIdTimeWithCurrentTimeIfNeeded() = clock.millis().also { if (it > idTime) idTime = it }

    private fun counterIsOverflow(counterValue: Long) = counterValue > bitsConfiguration.counterPartMask
}