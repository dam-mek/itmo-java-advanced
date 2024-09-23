package info.kgeorgiy.ja.denisov.hello;

import info.kgeorgiy.java.advanced.hello.HelloClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * HelloUDPClient is a simple UDP client that sends requests to a specified server.
 */
public class HelloUDPClient implements HelloClient {

    public static final int SO_TIMEOUT = 1000;

    /**
     * The main method that runs the HelloUDPClient.
     *
     * @param args the command line arguments:
     *             <ul>
     *                 <li>args[0] - the host name or IP address of the server</li>
     *                 <li>args[1] - the port number to send requests to</li>
     *                 <li>args[2] - the prefix for requests</li>
     *                 <li>args[3] - the number of parallel request threads</li>
     *                 <li>args[4] - the number of requests per thread</li>
     *             </ul>
     */
    public static void main(String[] args) {
        if (args.length != 5) {
            System.err.println("usage: HelloUDPClient <host> <port> <prefix> <threads> <requests>");
            return;
        }

        String host;
        int port;
        String prefix;
        int threads;
        int requests;

        try {
            host = args[0];
            port = Integer.parseInt(args[1]);
            prefix = args[2];
            threads = Integer.parseInt(args[3]);
            requests = Integer.parseInt(args[4]);
        } catch (NumberFormatException e) {
            System.err.println("port, threads and requests should be integers");
            return;
        }

        new HelloUDPClient().run(host, port, prefix, threads, requests);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run(String host, int port, String prefix, int threads, int requests) {
        SocketAddress socket;
        try {
            socket = new InetSocketAddress(InetAddress.getByName(host), port);
        } catch (UnknownHostException e) {
            System.err.println("unknown host " + host + ": " + e.getMessage());
            return;
        }
        CountDownLatch counter = new CountDownLatch(threads);
        ExecutorService executors = Executors.newFixedThreadPool(threads);
        for (int threadNumber = 1; threadNumber <= threads; threadNumber++) {
            executors.submit(getExecutor(prefix, requests, socket, threadNumber, counter));
        }
        try {
            counter.await();
        } catch (InterruptedException ignored) {
        }

        executors.shutdown();
        try {
            executors.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            executors.shutdownNow();
        }
    }

    private static Runnable getExecutor(String prefix, int requests, SocketAddress socketAddress, int threadNumber, CountDownLatch counter) {
        return () -> {
            try (DatagramSocket socket = new DatagramSocket()) {
                int receiveBufSize = socket.getReceiveBufferSize();
                socket.setSoTimeout(SO_TIMEOUT);
                DatagramPacket request = new DatagramPacket(new byte[0], 0, socketAddress);
                DatagramPacket response = new DatagramPacket(new byte[receiveBufSize], receiveBufSize);
                for (int requestNumber = 1; requestNumber <= requests; requestNumber++) {
                    byte[] requestBytes = execute(prefix, threadNumber, requestNumber); // :NOTE: naming
                    String stringResponse = "";
                    String expected = "Hello, " + prefix + threadNumber + "_" + requestNumber;
                    do {
                        request.setData(requestBytes);
                        try {
                            socket.send(request);
                        } catch (IOException e) {
                            System.err.println("can't send request: " + e.getMessage());
                            continue;
                        }
                        try {
                            socket.receive(response);
                            stringResponse = new String(response.getData(), response.getOffset(),
                                    response.getLength(), // :NOTE: вынести
                                    StandardCharsets.UTF_8);
                        } catch (IOException e) {
                            System.err.println("Error until receiving response: " + e.getMessage());
                        }
                    } while (!checkResponseText(stringResponse, expected));

                    String requestStr = prefix + threadNumber + "_" + requestNumber; // :NOTE: ??
                    System.out.println("Request: " + requestStr + "\nresponse: " + stringResponse + "\n\n");
                }
            } catch (SocketException e) {
                throw new RuntimeException(e);
            } finally {
                counter.countDown();
            }
        };
    }

    private static boolean checkResponseText(String response, String expected) {
        return response.chars()
                .mapToObj(c -> (char) c)
                .map(c -> Character.isDigit(c) ? Integer.toString(Character.getNumericValue(c)) : "" + c)
                .collect(Collectors.joining())
                .equals(expected);
    }

    private static byte[] execute(String prefix, int threadNumber, int requestNumber) {
        return (prefix + threadNumber + "_" + requestNumber).getBytes(StandardCharsets.UTF_8);
    }
}
