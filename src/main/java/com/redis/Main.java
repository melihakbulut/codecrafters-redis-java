package com.redis;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.reflections.Reflections;

public class Main {

    private static Configuration configuration;

    public static Configuration getConfiguration() {
        return configuration;
    }

    public static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for (byte b : a)
            sb.append(String.format("%02x", b));
        return sb.toString();
    }

    public static Pair parseAsPair(ByteBuffer buffer) {
        Long expiry = null;
        if (buffer.get() == -4) {
            expiry = buffer.getLong();
            buffer.get();
        }
        String key = null;
        String value = null;
        int keyLength = buffer.get();
        byte[] keyBuffer = new byte[keyLength];
        buffer.get(keyBuffer, 0, keyLength);
        key = new String(keyBuffer);

        int valueLength = buffer.get();
        byte[] valueBuffer = new byte[valueLength];
        buffer.get(valueBuffer, 0, valueLength);
        value = new String(valueBuffer);
        return Pair.builder().key(key).value(value).expiry(expiry).build();
    }

    private static void registerCommands() {
        Reflections reflections = new Reflections("com.redis");

        //        Set<Method> methods = reflections
        //          .getMethodsAnnotatedWith(Command.class);
        //        List<String> annotatedMethods = methods.stream()
        //          .map(method -> method.getAnnotation(Command.class)
        //          .value())
        //          .collect(Collectors.toList());

        Set<Class<?>> types = reflections.getTypesAnnotatedWith(Command.class);
        Map<String, Class<?>> commandMap = new HashMap<String, Class<?>>();
        for (Class<?> type : types) {
            commandMap.put(type.getAnnotation(Command.class).value(), type);
        }

        System.out.println(commandMap);
    }

    public static void main(String[] args) {
        registerCommands();

        //        byte[] arr = new byte[] {82, 69, 68, 73, 83, 48, 48, 48, 51, -6, 10, 114, 101, 100, 105,
        //                                 115, 45, 98, 105, 116, 115, -64, 64, -6, 9, 114, 101, 100, 105,
        //                                 115, 45, 118, 101, 114, 5, 55, 46, 50, 46, 48, -2, 0, -5, 1, 0, 0,
        //                                 9, 112, 105, 110, 101, 97, 112, 112, 108, 101, 4, 112, 101, 97,
        //                                 114, -1, -34, 113, -19, 22, -2, 83, -65, -35, 10};

        //        byte[] arr = new byte[] {82, 69, 68, 73, 83, 48, 48, 48, 51, -6, 9, 114, 101, 100, 105, 115,
        //                                 45, 118, 101, 114, 5, 55, 46, 50, 46, 48, -6, 10, 114, 101, 100,
        //                                 105, 115, 45, 98, 105, 116, 115, -64, 64, -2, 0, -5, 5, 0, 0, 4,
        //                                 112, 101, 97, 114, 9, 112, 105, 110, 101, 97, 112, 112, 108, 101,
        //                                 0, 9, 112, 105, 110, 101, 97, 112, 112, 108, 101, 4, 112, 101, 97,
        //                                 114, 0, 5, 103, 114, 97, 112, 101, 5, 109, 97, 110, 103, 111, 0, 5,
        //                                 97, 112, 112, 108, 101, 6, 98, 97, 110, 97, 110, 97, 0, 5, 109, 97,
        //                                 110, 103, 111, 10, 115, 116, 114, 97, 119, 98, 101, 114, 114, 121,
        //                                 -1, 7, 10, -26, 23, 89, 33, -96, 46, 10};
        //        byte[] arr = new byte[] {82, 69, 68, 73, 83, 48, 48, 48, 51, -6, 9, 114, 101, 100, 105, 115,
        //                                 45, 118, 101, 114, 5, 55, 46, 50, 46, 48, -6, 10, 114, 101, 100,
        //                                 105, 115, 45, 98, 105, 116, 115, -64, 64, -2, 0, -5, 4, 4, -4, 0,
        //                                 12, 40, -118, -57, 1, 0, 0, 0, 4, 112, 101, 97, 114, 6, 98, 97,
        //                                 110, 97, 110, 97, -4, 0, -100, -17, 18, 126, 1, 0, 0, 0, 9, 98,
        //                                 108, 117, 101, 98, 101, 114, 114, 121, 6, 111, 114, 97, 110, 103,
        //                                 101, -4, 0, 12, 40, -118, -57, 1, 0, 0, 0, 5, 103, 114, 97, 112,
        //                                 101, 9, 112, 105, 110, 101, 97, 112, 112, 108, 101, -4, 0, 12, 40,
        //                                 -118, -57, 1, 0, 0, 0, 9, 114, 97, 115, 112, 98, 101, 114, 114,
        //                                 121, 5, 97, 112, 112, 108, 101, -1, -106, 87, -18, 64, 58, -8, 2,
        //                                 71, 10};
        //
        //        System.out.println(Arrays.toString("banana".getBytes()));
        //        System.out.println((byte) 0xFC);
        //        System.out.println(ByteBuffer.wrap(new byte[] {0, 12, 40, -118, -57, 1, 0, 0})
        //                        .order(ByteOrder.LITTLE_ENDIAN).getLong());
        //        System.out.println(ByteBuffer.wrap(new byte[] {0, -100, -17, 18, 126, 1, 0, 0})
        //                        .order(ByteOrder.LITTLE_ENDIAN).getLong());
        //        System.out.println(ByteBuffer.wrap(new byte[] {0, -100, -17, 18, 126, 1, 0, 0, 0})
        //                        .order(ByteOrder.LITTLE_ENDIAN).getLong());
        //
        //        byte[] buf = arr;
        //        int index = 0;
        //        while (true) {
        //            try {
        //                if (buf[index - 3] == -5) {
        //                    int pairCount = buf[index - 2];
        //                    ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
        //                    byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        //                    byteBuffer.position(index);
        //                    for (int i = 0; i < pairCount; i++) {
        //                        System.out.println(parseAsPair(byteBuffer));
        //                    }
        //
        //                    break;
        //
        //                }
        //            } catch (Exception e) {
        //
        //            }
        //            index++;
        //        }

        // You can use print statements as follows for debugging, they'll be visible when running tests.
        System.out.println("Logs from your program will appear here!");
        Integer port = 6379;
        String replicaOf = null;
        String dir = null;
        String dbFileName = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--port")) {
                port = Integer.valueOf(args[i + 1]);
            } else if (args[i].equals("--replicaof")) {
                replicaOf = args[i + 1];
            } else if (args[i].equals("--dir")) {
                dir = args[i + 1];
            } else if (args[i].equals("--dbfilename")) {
                dbFileName = args[i + 1];
            }
        }
        configuration = Configuration.builder().replicaOf(replicaOf).port(port).dir(dir)
                        .dbFileName(dbFileName).build();

        String role = Objects.nonNull(configuration.getReplicaOf()) ? "slave" : "master";
        Replication replication = new Replication(configuration, role);

        //  Uncomment this block to pass the first stage
        ServerSocket serverSocket = null;
        Socket clientSocket = null;
        try {
            serverSocket = new ServerSocket(port);
            serverSocket.setReuseAddress(true);
            while (true) {
                // Wait for connection from client.
                clientSocket = serverSocket.accept();
                new Thread(new RedisHandler(clientSocket, replication)).start();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        } finally {
            try {
                if (clientSocket != null) {
                    clientSocket.close();
                }
            } catch (IOException e) {
                System.out.println("IOException: " + e.getMessage());
            }
        }
    }
}
