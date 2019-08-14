package ru.fix.stdlib.id.generator

import java.time.Clock

class SynchronizedIdGenerator(
        private val bitsConfiguration: BitsConfiguration,
        private val startOfTime: Long,
        serverId: Long,
        private val clock: Clock,
        counter: Long = 0L
) : IdGenerator {

    private val serverIdPart: Long = serverId and bitsConfiguration.serverPartMaxNumber
    private var counter: Long
    private var idTime: Long = 0L

    init {
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
        val tsPart = ((safeTime() - startOfTime) and bitsConfiguration.timePartMaxNumber) shl bitsConfiguration.serverPartBits + bitsConfiguration.counterPartBits
        val counterPart = (counterValue and bitsConfiguration.counterPartMaxNumber) shl bitsConfiguration.serverPartBits

        return tsPart or counterPart or serverIdPart
    }

    private fun safeTime(): Long {
        val now = clock.millis()
        if (idTime < now) {
            idTime = now
        }
        return idTime
    }

    private fun counterIsOverflow(counterValue: Long): Boolean {
        return counterValue > bitsConfiguration.counterPartMaxNumber
    }
}