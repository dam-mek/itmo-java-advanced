package info.kgeorgiy.ja.denisov.hello;

import info.kgeorgiy.java.advanced.hello.NewHelloServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.DatagramChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;


/**
 * HelloUDPNonblockingServer is a UDP server that handles requests and sends responses based on a predefined format.
 */
public class HelloUDPNonblockingServer implements NewHelloServer {
    private Selector selector;
    private ExecutorService workers;
    private final Map<Integer, DatagramChannel> channels = new HashMap<>();

    private static class ServerContext {
        final String responseFormat;
        ByteBuffer buffer;

        ServerContext(final String responseFormat, final int receiveBufferSize) {
            this.responseFormat = responseFormat;
            this.buffer = ByteBuffer.allocate(receiveBufferSize);
        }
    }

    /**
     * The main method that runs the HelloUDPNonblockingServer.
     *
     * @param args the command line arguments:
     *             <ul>
     *                 <li>args[0] - the port number to accept requests</li>
     *                 <li>args[1] - the number of worker threads to handle requests</li>
     *             </ul>
     */
    public static void main(final String[] args) {
        // :NOTE: args = null + same
        if (args.length != 2) {
            System.err.println("usage: HelloUDPNonblockingServer <port> <threads>");
            return;
        }

        final int port;
        final int threads;

        try {
            port = Integer.parseInt(args[0]);
            threads = Integer.parseInt(args[1]);
        } catch (final NumberFormatException e) {
            System.err.println("port and threads should be integers");
            return;
        }

        // :NOTE: try-with-resources
        new HelloUDPNonblockingServer().start(port, threads);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(final int threads, final Map<Integer, String> ports) {
        if (ports.isEmpty()) {
            System.err.println("ports can't be empty");
            return;
        }
        workers = Executors.newFixedThreadPool(threads);

        try {
            selector = Selector.open();
            for (final Map.Entry<Integer, String> entry : ports.entrySet()) {
                final DatagramChannel channel = DatagramChannel.open();
                channel.configureBlocking(false);
                channel.bind(new InetSocketAddress(entry.getKey()));
                channel.register(
                        selector,
                        SelectionKey.OP_READ,
                        new ServerContext(entry.getValue(), channel.socket().getReceiveBufferSize()));
                channels.put(entry.getKey(), channel);
            }
            workers.submit(this::receiver);

        } catch (final IOException e) {
            System.err.println("failed to start server: " + e.getMessage());
            close();
        }
    }

    private void receiver() {
        try {
            while (!Thread.interrupted() && selector.isOpen()) {
                selector.select();
                final Set<SelectionKey> selectedKeys = selector.selectedKeys();
                final Iterator<SelectionKey> iterator = selectedKeys.iterator();

                while (iterator.hasNext()) {
                    final SelectionKey key = iterator.next();
                    iterator.remove();

                    if (key.isReadable()) {
                        work(key);
                    }
                }
            }
        } catch (final IOException e) {
            System.err.println("Server error: " + e.getMessage());
        }
    }

    private void work(final SelectionKey key) {
        final DatagramChannel datagramChannel = (DatagramChannel) key.channel();
        final ServerContext context = (ServerContext) key.attachment();

        try {
            final InetSocketAddress clientAddress = (InetSocketAddress) datagramChannel.receive(context.buffer);
            if (clientAddress != null) {
                context.buffer.flip();
                final String packetData = StandardCharsets.UTF_8.decode(context.buffer).toString();
                final byte[] responseData = context.responseFormat.replaceAll("\\$", packetData).getBytes(StandardCharsets.UTF_8);
                context.buffer.clear();
                context.buffer.put(responseData);
                context.buffer.flip();
                datagramChannel.send(context.buffer, clientAddress);
                context.buffer.clear();
            }
        } catch (final IOException e) {
            System.err.println("failed to handle read: " + e.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        if (selector != null) {
            try {
                selector.close();
            } catch (final IOException e) {
                throw new RuntimeException(e); // :NOTE: log + ignore
            }
        }

        for (final DatagramChannel channel : channels.values()) {
            try {
                channel.close();
            } catch (final IOException e) {
                System.err.println("failed to close channel: " + e.getMessage());
            }
        }
        if (workers != null) {
            workers.close();
        }
    }
}
