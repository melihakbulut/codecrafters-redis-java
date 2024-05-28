import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Objects;

public class Main {

    private static Data data;

    public static Data getData() {
        return data;
    }

    public static void main(String[] args) {
        byte[] arr = new byte[] {82, 69, 68, 73, 83, 48, 48, 48, 51, -6, 10, 114, 101, 100, 105,
                                 115, 45, 98, 105, 116, 115, -64, 64, -6, 9, 114, 101, 100, 105,
                                 115, 45, 118, 101, 114, 5, 55, 46, 50, 46, 48, -2, 0, -5, 1, 0, 0,
                                 9, 112, 105, 110, 101, 97, 112, 112, 108, 101, 4, 112, 101, 97,
                                 114, -1, -34, 113, -19, 22, -2, 83, -65, -35, 10};
        System.out.println(Arrays.toString("pineapple".getBytes()));
        System.out.println(Arrays.toString("pear".getBytes()));
        System.out.println(new String(arr));

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
