package ru.fix.stdlib.id.generator

class BitsConfiguration(
        val serverPartBits: Int,
        val timePartBits: Int,
        val counterPartBits: Int
) {

    val serverPartMaxNumber: Long
    val timePartMaxNumber: Long
    val counterPartMaxNumber: Long

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

        serverPartMaxNumber = maxNumberForBits(serverPartBits)
        timePartMaxNumber = maxNumberForBits(timePartBits)
        counterPartMaxNumber = maxNumberForBits(counterPartBits)

    }

    private fun maxNumberForBits(bits: Int): Long  = (1L shl bits) - 1
}