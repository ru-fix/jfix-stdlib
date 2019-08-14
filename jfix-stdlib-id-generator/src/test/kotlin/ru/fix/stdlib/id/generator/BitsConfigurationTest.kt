package ru.fix.stdlib.id.generator

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

//TODO: написать параметризированые тесты
class BitsConfigurationTest {

    @Test
    fun `create bits configuration`() {

        val bitsPart = BitsConfiguration(1, 1, 62)

        assertEquals(1, bitsPart.serverPartBits)
        assertEquals(1, bitsPart.timePartBits)
        assertEquals(62, bitsPart.counterPartBits)

        assertEquals(1, bitsPart.serverPartMaxNumber)
        assertEquals(1, bitsPart.timePartMaxNumber)
        assertEquals(4611686018427387903, bitsPart.counterPartMaxNumber)
    }

    @Test
    fun `error when create bits configuration bits sum is more than 64`() {
        assertThrows(Exception::class.java) {
            BitsConfiguration(1, 2, 62)
        }
    }

    @Test
    fun `error when create bits configuration with negative value`() {
        assertThrows(Exception::class.java) {
            BitsConfiguration(-1, 2, 62)
        }
    }

    @Test
    fun `error when create bits configuration with zero value`() {
        assertThrows(Exception::class.java) {
            BitsConfiguration(1, 0, 62)
        }
    }
}