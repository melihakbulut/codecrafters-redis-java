import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;

public class Main {

    private static Data data;

    public static Data getData() {
        return data;
    }

    public static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for (byte b : a)
            sb.append(String.format("%02x", b));
        return sb.toString();
    }

    public static void main(String[] args) {
        //        byte[] arr = new byte[] {82, 69, 68, 73, 83, 48, 48, 48, 51, -6, 10, 114, 101, 100, 105,
        //                                 115, 45, 98, 105, 116, 115, -64, 64, -6, 9, 114, 101, 100, 105,
        //                                 115, 45, 118, 101, 114, 5, 55, 46, 50, 46, 48, -2, 0, -5, 1, 0, 0,
        //                                 9, 112, 105, 110, 101, 97, 112, 112, 108, 101, 4, 112, 101, 97,
        //                                 114, -1, -34, 113, -19, 22, -2, 83, -65, -35, 10};
        //
        //        String key = null;
        //        String value = null;
        //        byte[] buf = arr;
        //        int index = 0;
        //        while (true) {
        //            try {
        //                if (buf[index - 3] == -5 && buf[index - 2] == 1 && buf[index - 1] == 0
        //                    && buf[index] == 0) {
        //                    int keyLength = buf[++index];
        //                    byte[] keyBuffer = new byte[keyLength];
        //                    System.arraycopy(buf, index + 1, keyBuffer, 0, keyLength);
        //                    key = new String(keyBuffer);
        //                    index += keyLength;
        //
        //                    int valueLength = buf[++index];
        //                    byte[] valueBuffer = new byte[valueLength];
        //                    System.arraycopy(buf, index + 1, valueBuffer, 0, valueLength);
        //                    value = new String(valueBuffer);
        //                    index += valueLength;
        //                    break;
        //
        //                }
        //            } catch (Exception e) {
        //
        //            }
        //
        //            index++;
        //        }
        //        System.out.println(key + ":" + value);

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
        Configuration configuration = Configuration.builder().replicaOf(replicaOf).port(port)
                        .dir(dir).dbFileName(dbFileName).build();
        data = new Data(configuration);
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
                new Thread(new RedisHandler(clientSocket, configuration, replication)).start();
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
