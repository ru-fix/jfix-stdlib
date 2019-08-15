package ru.fix.stdlib.id.generator

/**
 * Bits configuration for [IdGenerator]
 *
 * Total count of bits cannot be greater than 64
 *
 * @param timePartBits number of bits for time part
 * @param counterPartBits number of bits for counter part
 * @param serverPartBits number of bits for server part
 *
 * @see IdGenerator
 * @see SynchronizedIdGenerator
 * @see ReadWriteLockIdGenerator
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