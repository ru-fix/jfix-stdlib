package ru.fix.crudility.infra

import org.apache.logging.log4j.kotlin.Logging
import java.nio.file.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


/**
 * @param filePaths List of file paths to listen
 */
class FileWatcher : AutoCloseable {

    companion object : Logging

    private val watchThread = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, FileWatcher.javaClass.name)
    }

    val dirToKey = mutableMapOf<Path, WatchKey>()
    val keyToDir = mutableMapOf<WatchKey, Path>()
    val keyToFileNameListeners = mutableMapOf<WatchKey, HashMap<Path, (Path) -> Unit>>()

    val watchService = FileSystems.getDefault().newWatchService()

    fun register(filePath: Path, listener: (Path) -> Unit) {
        val dir = filePath.parent

        if (!dirToKey.contains(dir)) {
            val key = dir.register(watchService, StandardWatchEventKinds.ENTRY_MODIFY)

            dirToKey[dir] = key
            keyToDir[key] = dir
        }
        keyToFileNameListeners
                .computeIfAbsent(dirToKey.getValue(dir)) { HashMap() }
                .put(filePath.fileName, listener)
    }

    fun unregister(filePath: Path) {
        val dir = filePath.parent
        val key = dirToKey[dir] ?: return

        keyToFileNameListeners.compute(key) { _, fileNameListeners ->
            requireNotNull(fileNameListeners)

            fileNameListeners.remove(filePath.fileName)
            if (fileNameListeners.isNotEmpty()) {
                fileNameListeners
            } else {
                //last file name for the directory
                dirToKey.remove(dir)
                key.cancel()
                null
            }
        }
    }

    init {
        watchThread.submit {
            try {
                while (!Thread.currentThread().isInterrupted) {

                    val watchKey = watchService.take() ?: continue
                    val fileNameListeners = keyToFileNameListeners[watchKey] ?: continue

                    for (event in watchKey.pollEvents()) {

                        if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
                            val fileName = event.context() as? Path ?: continue
                            val listener = fileNameListeners[fileName] ?: continue

                            logger.info { "Detect modification for: $fileName" }
                            try {
                                listener.invoke(keyToDir.getValue(watchKey).resolve(fileName))
                            } catch (interrupted: InterruptedException) {
                                throw interrupted
                            } catch (exc: Exception) {
                                logger.error("Failed to invoke listener of file change: $fileName", exc)
                            }
                        }
                    }
                    watchKey.reset()
                }
            } catch (closed: ClosedWatchServiceException) {
                //stop processing
            } catch (interrupted: InterruptedException) {
                //stop processing
            } catch (exc: Exception) {
                logger.error(exc) { "Failed to watch for file changes" }
            }
        }
    }

    override fun close() {
        watchService.close()
        watchThread.shutdown()
        if (!watchThread.awaitTermination(1, TimeUnit.MINUTES)) {
            logger.error("Failed to await watch service close")
        }
        watchThread.shutdownNow()
        if (!watchThread.awaitTermination(1, TimeUnit.MINUTES)) {
            logger.error("Failed to await termination of watcher thread")
        }
    }
}