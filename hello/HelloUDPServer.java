package info.kgeorgiy.ja.denisov.hello;

import info.kgeorgiy.java.advanced.hello.NewHelloServer;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;



/**
 * HelloUDPServer is a UDP server that handles requests and sends responses based on a predefined format.
 */
public class HelloUDPServer implements NewHelloServer {
    List<DatagramSocket> sockets;
    private ExecutorService receivers;
    private ExecutorService workers;
    /**
     * The main method that runs the HelloUDPServer.
     *
     * @param args the command line arguments:
     *             <ul>
     *                 <li>args[0] - the port number to accept requests</li>
     *                 <li>args[1] - the number of worker threads to handle requests</li>
     *             </ul>
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("usage: HelloUDPServer <port> <threads>");
            return;
        }

        int port;
        int threads;

        try {
            port = Integer.parseInt(args[0]);
            threads = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.err.println("port and threads should be integers");
            return;
        }

        new HelloUDPServer().start(port, threads);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(int threads, Map<Integer, String> ports) {
        if (ports.isEmpty()) {
            System.err.println("ports can't be empty");
            return;
        }
        workers = Executors.newFixedThreadPool(threads);
        receivers = Executors.newFixedThreadPool(ports.size());
        sockets = new ArrayList<>();
        for (Integer port : ports.keySet()) {
            DatagramSocket socket;
            try {
                socket = new DatagramSocket(port);
                sockets.add(socket);
                receivers.submit(getReceiver(ports.get(port), socket));
            } catch (SocketException e) {
                System.err.println("can't create socket with port " + port);
                close();
                return;
            }
        }
    }

    private Runnable getReceiver(String answerFormat, DatagramSocket socket) {
        return () -> {
            while (!socket.isClosed()) {
                int receiveBufSize;
                try {
                    receiveBufSize = socket.getReceiveBufferSize();
                } catch (SocketException e) {
                    System.err.println("invalid socket: " + e.getLocalizedMessage());
                    return;
                }
                DatagramPacket packet = new DatagramPacket(new byte[receiveBufSize], receiveBufSize);
                try {
                    socket.receive(packet);
                } catch (IOException e) {
                    if (!socket.isClosed()) {
                        System.err.println("can't receive packet from socket: " + e.getMessage());
                    }
                }
                workers.submit(getWorker(answerFormat, socket, packet));
            }
        };
    }

    private static Runnable getWorker(String answerFormat, DatagramSocket socket, DatagramPacket packet) {
        return () -> {
            String packetData = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8); // :NOTE: общие с клиентом
            byte[] responseData = answerFormat.replaceAll("\\$", packetData).getBytes(StandardCharsets.UTF_8);
            DatagramPacket response = new DatagramPacket(responseData, responseData.length, packet.getSocketAddress());
            try {
                socket.send(response);
            } catch (IOException e) {
                if (!socket.isClosed()) {
                    System.err.println("can't send response: " + e.getMessage());
                }
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (sockets == null) {
            return;
        }
        for (DatagramSocket socket : sockets) {
            socket.close();
        }
        receivers.close();
        workers.close();
    }
}
