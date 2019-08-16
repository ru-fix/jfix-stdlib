package ru.fix.stdlib.socket.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProxySocket implements AutoCloseable {

    private static Logger log = LoggerFactory.getLogger(ProxySocket.class);

    private String destinationHost;
    private int destinationPort;
    private int sourcePort;
    private ExecutorService executorService;

    private ServerSocket sourceServerSocket;
    private AtomicBoolean isShutdown = new AtomicBoolean();

    /**
     * @param executorService provide thread pools for connections. Each connection to the socket
     *                        use separate thread from executorService's thread pool
     */
    public ProxySocket(String destinationHost, int destinationPort,
                       int sourcePort, ExecutorService executorService) throws IOException {
        this.destinationHost = destinationHost;
        this.destinationPort = destinationPort;
        this.sourcePort = sourcePort;
        this.executorService = executorService;

        start();
    }

    private void start() throws IOException {
        sourceServerSocket = new ServerSocket(sourcePort);
        executorService.submit(() -> {
                    final byte[] request = new byte[1024];
                    final byte[] reply = new byte[4096];

                    while (!isShutdown.get()) {
                        try (Socket sourceSocket = sourceServerSocket.accept();
                             final InputStream streamFromClient = sourceSocket.getInputStream();
                             final OutputStream streamToClient = sourceSocket.getOutputStream();
                             Socket destinationSocket = new Socket(destinationHost, destinationPort)) {
                            executorService.submit(() -> {
                                try (OutputStream streamToServer = destinationSocket.getOutputStream()) {
                                    int bytesRead;
                                    while (!isShutdown.get() && (bytesRead = streamFromClient.read(request)) != -1) {
                                        streamToServer.write(request, 0, bytesRead);
                                        streamToServer.flush();
                                    }
                                } catch (IOException e) {
                                    log.error("Failed to flush to dest {}", e);
                                }
                            });

                            try (InputStream streamFromServer = destinationSocket.getInputStream()) {
                                int bytesRead;
                                while (!isShutdown.get() && (bytesRead = streamFromServer.read(reply)) != -1) {
                                    streamToClient.write(reply, 0, bytesRead);
                                    streamToClient.flush();
                                }
                            } catch (IOException e) {
                                log.error("Failed to flush to client", e);
                            }
                        } catch (IOException e) {
                            log.error("Failed to open socket", e);
                        }
                    }
                }
        );
    }

    public int getPort() {
        return sourceServerSocket.getLocalPort();
    }

    @Override
    public void close() throws InterruptedException {
        try {
            sourceServerSocket.close();
            executorService.shutdown();
        } catch (IOException e) {
            executorService.awaitTermination(5, TimeUnit.SECONDS);
            log.error("Error while trying to close socket: " + e);
            log.info("Force executor service termination");
            executorService.shutdownNow();
        }
        isShutdown.set(true);
    }
}
