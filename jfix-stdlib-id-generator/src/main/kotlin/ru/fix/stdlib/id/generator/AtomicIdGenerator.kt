package ru.fix.stdlib.id.generator

import org.apache.logging.log4j.kotlin.Logging
import java.time.Clock
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

/**
 * Generates growing unique identifier.
 */
class AtomicIdGenerator(
    private val bitsConfig: BitsConfiguration,
    private val startOfTime: Long,
    serverId: Long,
    private val clock: Clock
) : IdGenerator {

    companion object : Logging

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
            if (java.lang.Long.highestOneBit((currentTime - startOfTime)) > bitsConfig.timePartMask) {
                logger.warn {
                    """
                        Id generator configured with ${bitsConfig.timePartBits} bits for timestamp part.
                        That is not enough to hold current timestamp with configured started time
                        ${Instant.ofEpochMilli(startOfTime)}
                    """.trimIndent()
                }
            }
            require(startOfTime in 0..currentTime) {
                "startOfTime $startOfTime should be non negative and cannot be greater than current time $currentTime"
            }
        }

        id = AtomicLong(buildIdForNewTimestampWithZeroCounter(clock.millis()))
    }

    private fun buildIdForNewTimestampWithZeroCounter(timeValue: Long): Long {
        val timestampPart =
            ((timeValue - startOfTime) and bitsConfig.timePartMask) shl (bitsConfig.serverPartBits + bitsConfig.counterPartBits)
        return timestampPart or serverIdPart
    }

    private fun timeFromId(idValue: Long): Long =
        startOfTime + (idValue shr (bitsConfig.serverPartBits + bitsConfig.counterPartBits)) and bitsConfig.timePartMask


    override fun nextId(): Long {
        /*
        Example:
        time part: 3 bit 0b110
        counter part 2 bit 0b11
        server_id 3 bit 0b101
        previously generated identifier inside atomic id: 0b110_11_101

        1. If time part of id is same as current time, e.g. 0b110 then we will increment counter part:
            0b110_11_101
            +
            ob000_01_000
            =
            0b111_00_101
        Counter overflow will lead to increment of time part to the future.

        2. If time part of id is lower than current time, then we will update whole id by CAS
            0b110_11_101
            CAS TO
            0b111_00_101
            =
            0b111_00_101
        */

        while (true) {
            val idValue = id.get()
            val idTime = timeFromId(idValue)
            val currentTime = clock.millis() and bitsConfig.timePartMask

            if (idTime >= currentTime) {
                return id.addAndGet(bitsConfig.serverPartMask + 1)
            } else {
                //Catch up id time with current time
                val newIdValue = buildIdForNewTimestampWithZeroCounter(currentTime)

                if (id.weakCompareAndSetVolatile(idValue, newIdValue)) {
                    return newIdValue
                } else {
                    //id was updated by other thread, try again
                    continue
                }
            }
        }
    }

    fun extractTimePart(id: Long): Long {
        val shiftBits = bitsConfig.serverPartBits + bitsConfig.counterPartBits
        return startOfTime + extractPart(id, shiftBits, bitsConfig.timePartMask)
    }

    fun extractServerIdPart(id: Long) = extractPart(id, 0, bitsConfig.serverPartMask)

    fun extractCounterPart(id: Long) = extractPart(id, bitsConfig.serverPartBits, bitsConfig.counterPartMask)

    private fun extractPart(id: Long, shiftBits: Int, mask: Long) = id shr shiftBits and mask
}

