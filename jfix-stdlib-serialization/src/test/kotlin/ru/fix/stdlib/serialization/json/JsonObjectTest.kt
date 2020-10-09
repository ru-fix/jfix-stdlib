package ru.fix.stdlib.serialization.json

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.junit.jupiter.api.Test

class JsonObjectTest {

    @Test
    fun `unknown fields are mapped to unmappedFields property`() {
        val json = """
            {
                "someField": "123",
                "someUnknownField": "str",
                "unmappedFields": "1234"
            }
        """.trimIndent()

        val result = Marshaller.unmarshall(json, TestData::class.java)

        result shouldNotBe null
        result.otherFields()["someUnknownField"] shouldBe "str"
        result.otherFields()["unmappedFields"] shouldBe "1234"
    }

    @Test
    fun `unmappedFields are serialized to json`() {
        val data = TestData(
                someField = "123"
        ).apply {
            this.setOtherField("someUnknownField", "str")
        }

        val json = Marshaller.marshall(data)
        json shouldBe """{"someField":"123","someUnknownField":"str"}"""
    }

    @Test
    fun `result without unknown fields is correctly mapped`() {
        val json = """
            {
                "someField": "123"
            }
        """.trimIndent()

        val result = Marshaller.unmarshall(json, TestData::class.java)

        result shouldNotBe null
    }

    @Test
    fun `dto without unmappedFields is serialized to json`() {
        val data = TestData(
                someField = "123"
        )

        val json = Marshaller.marshall(data)
        json shouldBe """{"someField":"123"}"""
    }

    @Test
    fun `reified type`() {
        val json = """
            {
                "someField": "123",
                "someUnknownField": "str",
                "unmappedFields": "1234"
            }
        """.trimIndent()

        val result: TestData = Marshaller.unmarshall(json)

        result shouldNotBe null
        result.otherFields()["someUnknownField"] shouldBe "str"
        result.otherFields()["unmappedFields"] shouldBe "1234"
    }

    data class TestData(
            val someField: String
    ) : JsonObject()
}

object Marshaller {

    private val mapper: ObjectMapper = jacksonObjectMapper()

    fun marshall(serializedObject: Any): String {
        return mapper.writeValueAsString(serializedObject)
    }

    inline fun <reified T> unmarshall(json: String): T {
        return unmarshall(json, T::class.java)
    }

    fun <T> unmarshall(json: String, targetType: Class<T>): T {
        return mapper.readValue(json, targetType)
    }
}