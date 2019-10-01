package ru.fix.crudility.infra

import org.apache.logging.log4j.kotlin.Logging
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.nio.file.Files
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit


class FileWatcherTest {
    companion object : Logging

    @CsvSource("1", "3")
    @ParameterizedTest
    fun `notify when file content changed`(numberOfFiles: Int) {

        val files = (1..numberOfFiles).map {
            Files.createTempFile("file-watcher-test", ".txt")
                    .toFile()
                    .apply {
                        deleteOnExit()
                        writeText("init content")
                    }
        }

        val flags = files.map { it.toPath() to Semaphore(0) }.toMap()

        FileWatcher().use { watcher ->
            flags.keys.forEach { key ->
                watcher.register(key) { path ->
                    logger.info { "updated $path" }
                    flags.getValue(path).release()
                }
            }

            files.forEach { file ->
                logger.info { "change file $file" }
                file.writeText("change file here")
            }

            flags.forEach { (path, flag) ->
                assertTrue(flag.tryAcquire(15, TimeUnit.SECONDS)) {
                    "notification is not received on file content change for $path"
                }
            }
        }

        files.forEach { it.delete() }
    }
}



