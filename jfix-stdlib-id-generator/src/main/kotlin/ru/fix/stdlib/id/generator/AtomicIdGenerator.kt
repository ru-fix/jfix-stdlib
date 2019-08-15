package ru.fix.stdlib.id.generator

import java.time.Clock
import java.util.concurrent.atomic.AtomicLong

class AtomicIdGenerator(
        private val bitsConfig: BitsConfiguration,
        private val startOfTime: Long,
        serverId: Long,
        private val clock: Clock
) : IdGenerator {
    /**
     * Last used ID
     */
    private val id: AtomicLong
    private val serverIdPart = serverId

    init {
        require(serverId in 0..bitsConfig.serverPartMask) {
            "serverId $serverId should be non negative and cannot be greater than ${bitsConfig.serverPartMask}"
        }

        clock.millis().let { currentTime ->
            require(startOfTime in 0..currentTime) {
                "startOfTime $startOfTime should be non negative and cannot be greater than current time $currentTime"
            }
        }

        id = AtomicLong(buildId(clock.millis(), 0))
    }

    private fun buildId(timeValue: Long, counterValue: Long): Long{
            val timestampPart = ((timeValue - startOfTime) and bitsConfig.timePartMask) shl (bitsConfig.serverPartBits + bitsConfig.counterPartBits)
            val counterPart = (counterValue and bitsConfig.counterPartMask) shl bitsConfig.serverPartBits
            return timestampPart or counterPart or serverIdPart
    }

    private fun timeFromId(idValue: Long): Long =
        startOfTime + (idValue shr (bitsConfig.serverPartBits + bitsConfig.counterPartBits)) and bitsConfig.timePartMask


    override fun nextId(): Long {
        while(true) {
            val idValue = id.get()
            val idTime = timeFromId(idValue)
            val currentTime = clock.millis()

            if (idTime >= currentTime) {
                return id.incrementAndGet()
            } else {
                //Catch up id time with current time
                val newIdValue = buildId(currentTime, 0)

                //TODO: replace with compareAndExchange during update to JDK11
                if (id.compareAndSet(idValue, newIdValue)) {
                    return newIdValue
                } else {
                    //id was updated by other thread, try again
                    continue
                }
            }
        }
    }
}

