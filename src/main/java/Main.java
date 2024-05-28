import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Objects;

public class Main {

    private static Data data;

    public static Data getData() {
        return data;
    }

    public static void main(String[] args) {
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
