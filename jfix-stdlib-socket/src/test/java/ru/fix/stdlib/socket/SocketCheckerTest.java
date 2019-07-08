package ru.fix.stdlib.socket;

import org.junit.jupiter.api.Test;

import java.net.BindException;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.*;

public class SocketCheckerTest {

    @Test
    public void availability() throws Exception {
        for (int i = 0; i < 15; i++) {
            try {
                int port = SocketChecker.getAvailableRandomPort();
                assertTrue(SocketChecker.isAvailable(port));
                try (ServerSocket socket = new ServerSocket(port)) {
                    assertFalse(SocketChecker.isAvailable(port));
                }
                return;
            } catch (BindException e) {
                // ignore it
            }
        }
        fail("Port was busy all time");
    }
}
