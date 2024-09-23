package info.kgeorgiy.ja.denisov.hello;

import info.kgeorgiy.java.advanced.hello.HelloClient;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * HelloUDPNonblockingClient is a simple UDP client that sends requests to a specified server.
 */
public class HelloUDPNonblockingClient implements HelloClient {
    // :NOTE: Обобщить что-то с HelloUDPClient
    public static final int SO_TIMEOUT = 100;

    private static class ClientContext {
        final String prefix;
        final int threadNumber;
        final int requests;
        int currentRequest;
        ByteBuffer buffer;

        ClientContext(final String prefix, final int threadNumber, final int requests, final int receiveBufferSize) {
            this.prefix = prefix;
            this.threadNumber = threadNumber;
            this.requests = requests;
            this.currentRequest = 1;
            this.buffer = ByteBuffer.allocate(receiveBufferSize);
        }
    }

    /**
     * The main method that runs the HelloUDPNonblockingClient.
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
    public static void main(final String[] args) {
        if (args == null || args.length != 5) {
            System.err.println("usage: HelloUDPNonblockingClient <host> <port> <prefix> <threads> <requests>");
            return;
        }

        final String host;
        final int port;
        final String prefix;
        final int threads;
        final int requests;

        try {
            // :NOTE: args[i] = null
            host = args[0];
            port = Integer.parseInt(args[1]);
            prefix = args[2];
            threads = Integer.parseInt(args[3]);
            requests = Integer.parseInt(args[4]);
        } catch (final NumberFormatException e) {
            System.err.println("port, threads and requests should be integers: " + e.getLocalizedMessage());
            return;
        }

        new HelloUDPNonblockingClient().run(host, port, prefix, threads, requests);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run(final String host, final int port, final String prefix, final int threads, final int requests) {
        final SocketAddress socketAddress;
        try {
            socketAddress = new InetSocketAddress(InetAddress.getByName(host), port);
        } catch (final UnknownHostException e) {
            System.err.println("unknown host " + host + ": " + e.getMessage());
            return;
        }
        final List<DatagramChannel> channels = new ArrayList<>();
        try (final Selector selector = Selector.open()) {
            for (int i = 1; i <= threads; i++) {
                final DatagramChannel channel = DatagramChannel.open();
                channels.add(channel);
                channel.configureBlocking(false);
                channel.connect(socketAddress);
                channel.register(
                        selector,
                        SelectionKey.OP_WRITE,
                        new ClientContext(prefix, i, requests, channel.socket().getReceiveBufferSize())
                );
            }

            while (!selector.keys().isEmpty()) {
                selector.select(SO_TIMEOUT);
                if (selector.selectedKeys().isEmpty()) {
                    for (final SelectionKey key : selector.keys()) {
                        key.interestOps(SelectionKey.OP_WRITE);
                    }
                }
                final Set<SelectionKey> selectedKeys = selector.selectedKeys();
                final Iterator<SelectionKey> iterator = selectedKeys.iterator();

                while (iterator.hasNext()) {
                    final SelectionKey key = iterator.next();
                    iterator.remove();

                    if (key.isValid()) {
                        if (key.isWritable()) {
                            sendRequest(key);
                        } else if (key.isReadable()) {
                            receiveResponse(key);
                        }
                    }
                }
            }
        } catch (final IOException e) {
            System.err.println("client error: " + e.getLocalizedMessage());
        } finally {
            for (final DatagramChannel channel : channels) {
                try {
                    channel.close();
                } catch (final IOException e) {
                    System.err.println("can't close channel: " + e.getLocalizedMessage());
                }
            }
        }

    }

    private void sendRequest(final SelectionKey key) {
        final DatagramChannel channel = (DatagramChannel) key.channel();
        final ClientContext context = (ClientContext) key.attachment();
        if (context.currentRequest <= context.requests) {
            final byte[] requestBytes = makeRequest(context.prefix, context.threadNumber, context.currentRequest);
            final ByteBuffer buffer = ByteBuffer.wrap(requestBytes); // :NOTE: theory
            try {
                channel.send(buffer, channel.getRemoteAddress());
                key.interestOps(SelectionKey.OP_READ);
            } catch (final IOException e) {
                System.err.println("can't send: " + e.getLocalizedMessage());
            }
        } else {
            try {
                channel.close();
            } catch (final IOException ignored) {
            }
        }
    }


    private void receiveResponse(final SelectionKey key) {
        final DatagramChannel channel = (DatagramChannel) key.channel();
        final ClientContext context = (ClientContext) key.attachment();

        try {
            channel.receive(context.buffer);
        } catch (final IOException ignored) {
            return;
        }
        context.buffer.flip();
        final String responseString = StandardCharsets.UTF_8.decode(context.buffer).toString();
        context.buffer.clear();

        final String expectedResponse = "Hello, " + context.prefix + context.threadNumber + "_" + context.currentRequest;

        if (checkResponseText(responseString, expectedResponse)) {
            context.currentRequest++;
            key.interestOps(SelectionKey.OP_WRITE);
        } else {
            key.interestOps(SelectionKey.OP_READ);
        }
    }


    private static boolean checkResponseText(final String response, final String expected) {
        return response.chars()
                .mapToObj(c -> (char) c)
                .map(c -> Character.isDigit(c) ? Integer.toString(Character.getNumericValue(c)) : "" + c)
                .collect(Collectors.joining())
                .equals(expected);
    }

    private static byte[] makeRequest(final String prefix, final int threadNumber, final int requestNumber) {
        return (prefix + threadNumber + "_" + requestNumber).getBytes(StandardCharsets.UTF_8);
    }
}
