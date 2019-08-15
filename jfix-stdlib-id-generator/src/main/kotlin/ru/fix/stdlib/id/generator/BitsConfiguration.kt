package ru.fix.stdlib.id.generator

/**
 * Bits configuration for [IdGenerator]
 * IdGenerator generates a number that composed of three part in following order: [ timestamp | counter | server ], where:
 * timestamp  - current timestamp
 * counter - local server counter
 * server - unique number of a server or application instance or process that generated id
 *
 * Take into consideration that:
 * 43 bit timestamp will cover about 278 years.
 * 9 bit counter will flip every 512 iteration
 * 10 bit server number will allow to use 1024 unique application instances to generate ids concurrently
 * Prefer to use 63 bit keys in order to support growing ids in systems that do not allow unsigned long values.
 *
 * Total count of bits cannot be greater than 64
 *
 * @param timePartBits number of bits for time part
 * @param counterPartBits number of bits for counter part
 * @param serverPartBits number of bits for server part
 *
 * @see IdGenerator
 * @see AtomicIdGenerator
 */
class BitsConfiguration(
        val timePartBits: Int,
        val counterPartBits: Int,
        val serverPartBits: Int
) {
    val timePartMask: Long
    val counterPartMask: Long
    val serverPartMask: Long

    init {

        val totalBits = serverPartBits + timePartBits + counterPartBits

        check(totalBits <= 64) {
            "All parts must not be more than 64 bits, actual $totalBits"
        }

        check(serverPartBits > 0 && timePartBits > 0 && counterPartBits > 0) {
            """Number of bits must be greater than 0, 
                |serverPartBits=$serverPartBits, 
                |timePartBits=$timePartBits, 
                |counterPartBits=$counterPartBits""".trimMargin()
        }

        serverPartMask = maskForBits(serverPartBits)
        timePartMask = maskForBits(timePartBits)
        counterPartMask = maskForBits(counterPartBits)

    }

    private fun maskForBits(bits: Int): Long  = (1L shl bits) - 1
}