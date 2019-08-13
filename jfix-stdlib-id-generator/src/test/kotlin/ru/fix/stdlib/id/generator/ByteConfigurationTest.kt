package ru.fix.stdlib.id.generator

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

//TODO: написать параметризированые тесты
class ByteConfigurationTest {

    @Test
    fun `create byte configuration`() {

        val bytePart = ByteConfiguration(1, 1, 62)

        assertEquals(1, bytePart.serverPartBytes)
        assertEquals(1, bytePart.timePartBytes)
        assertEquals(62, bytePart.counterPartBytes)

        assertEquals(1, bytePart.serverPartMaxNumber)
        assertEquals(1, bytePart.timePartMaxNumber)
        assertEquals(4611686018427387903, bytePart.counterPartMaxNumber)
    }

    @Test
    fun `error when create byte configuration byte sum is more than 64`() {
        assertThrows(Exception::class.java) {
            ByteConfiguration(1, 2, 62)
        }
    }

    @Test
    fun `error when create byte configuration with negative value`() {
        assertThrows(Exception::class.java) {
            ByteConfiguration(-1, 2, 62)
        }
    }

    @Test
    fun `error when create byte configuration with zero value`() {
        assertThrows(Exception::class.java) {
            ByteConfiguration(1, 0, 62)
        }
    }
}