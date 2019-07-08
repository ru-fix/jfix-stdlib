package ru.fix.stdlib.socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.ThreadLocalRandom;

public class SocketChecker {

    private static final Logger log = LoggerFactory.getLogger(SocketChecker.class);

    private static final int PORTS_FROM = 30000;
    private static final int PORTS_TO = 60000;

    private SocketChecker() {
    }

    public static boolean isAvailable(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port);
             DatagramSocket datagramSocket = new DatagramSocket(port)) {

            return serverSocket.isBound() && datagramSocket.isBound();
        } catch (Exception exc) {
            /*
             * ignore exception
             */
            log.trace("Failed to occupy socket during socket availability check.", exc);
        }
        return false;
    }


    public static int getAvailableRandomPort() {
        for (int attempt = 0; attempt < 100; attempt++) {
            int port = ThreadLocalRandom.current().nextInt(PORTS_FROM, PORTS_TO);
            try (ServerSocket serverSocket = new ServerSocket();
                 DatagramSocket datagramSocket = new DatagramSocket(port)) {
                serverSocket.setReuseAddress(true);
                serverSocket.bind(new InetSocketAddress(port), 100);
                return port;
            } catch (Exception exc) {
                /*
                 * ignore exception
                 */
                log.trace("Failed to occupy socket during socket availability check.", exc);
            }
        }

        throw new RuntimeException("100 attempts failed to get available random port.");
    }

}
