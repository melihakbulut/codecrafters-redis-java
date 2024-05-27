import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisHandler implements Runnable {

    private Socket clientSocket;

    //    private static final Queue<String> replicationConnectQueue = new ArrayBlockingQueue<String>(10);
    //    private static final List<Socket> replications = new ArrayList<Socket>();

    private static final List<Socket> replications = new ArrayList<Socket>();
    private Replication replication;
    private Configuration configuration;
    private static final Data data = new Data();

    public static final String notFound = "$-1\r\n";

    private AtomicInteger offset = new AtomicInteger(0);
    private boolean handshakeDone = false;

    public RedisHandler(Socket clientSocket, Configuration configuration, Replication replication) {
        this.clientSocket = clientSocket;
        this.configuration = configuration;
        this.replication = replication;
    }

    @Override
    public void run() {
        try {
            listenSocket();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void listenSocket() throws IOException {
        while (true) {
            byte b = readByteFromSocket();
            if (b == '*') {
                int commandWordLength = Integer.valueOf(getStringValueOfByte(readByteFromSocket()));
                skipNewLine();
                String[] commandWords = new String[commandWordLength];
                for (int i = 0; i < commandWordLength; i++) {
                    commandWords[i] = parseCommand();
                }
                System.out.println(Arrays.toString(commandWords));
                handle(commandWords);
            }
        }
    }

    private void handle(String[] commandWords) throws IOException {
        String message = null;
        if (checkCommand(commandWords, "ping")) {
            message = "+PONG\r\n";
        } else if (checkCommand(commandWords, "command")
                   || checkCommand(commandWords, "replconf")) {
            if (checkCommand(commandWords, "replconf") && checkCommand(commandWords, "getack", 1)) {
                String offsetValue = null;
                if (!handshakeDone) {
                    offset.set(0);
                    handshakeDone = true;
                }

                offsetValue = String.valueOf(offset.get());

                message = String.format("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%s\r\n%s\r\n",
                                        offsetValue.length(), offsetValue);

            } else
                message = "+OK\r\n";
            //            if (commandWords.length == 3 && !commandWords[2].equals("psync2")) {
            //                String host = ((InetSocketAddress) clientSocket.getRemoteSocketAddress())
            //                                .getHostName();
            //                String port = commandWords[2];
            //                replicationConnectQueue.add(host + ":" + port);
            //            }
        } else if (checkCommand(commandWords, "echo")) {

            message = String.format("$%s\r\n%s\r\n", commandWords[1].length(), commandWords[1]);

        } else if (checkCommand(commandWords, "set")) {
            if (commandWords.length > 3)
                data.putMap(commandWords[1], commandWords[2], Long.parseLong(commandWords[4]));
            else
                data.putMap(commandWords[1], commandWords[2]);

            message = "+OK\r\n";

            if (replication.getKeyValueMap().get("role").equals("master")) {
                //                while (!replicationConnectQueue.isEmpty()) {
                //                    String replicationConfigItem = replicationConnectQueue.poll();
                //                    String[] arr = replicationConfigItem.split(":");
                //                    replications.add(new Socket(arr[0], Integer.valueOf(arr[1])));
                //                }

                String key = commandWords[1];
                String value = commandWords[2];
                String setFormat = "*3\r\n$3\r\nSET\r\n$%s\r\n%s\r\n$%s\r\n%s\r\n";
                String replicaMessage = String.format(setFormat, key.length(), key, value.length(),
                                                      value);
                for (Socket replica : replications) {
                    System.out.println("sent to replica " + replicaMessage);
                    replica.getOutputStream().write(replicaMessage.getBytes());
                }
            }

        } else if (checkCommand(commandWords, "get")) {
            try {

                String value = data.getFromMap(commandWords[1]);
                //                if (!value.isEmpty())
                message = String.format("$%s\r\n%s\r\n", value.length(), value);
                //                else
                //                    message = notFound;
            } catch (Exception e) {
                message = notFound;
            }
        } else if (checkCommand(commandWords, "info")) {
            if (checkCommand(commandWords, "replication", 1)) {
                message = convertToMessage(replication.getKeyValueMap());
            }
        } else if (checkCommand(commandWords, "psync")) {
            message = "+FULLRESYNC " + replication.getKeyValueMap().get("master_replid") + " "
                      + replication.getKeyValueMap().get("master_repl_offset") + "\r\n";
            String emptyHex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
            byte[] payload = HexFormat.of().parseHex(emptyHex);
            message += String.format("$%s\r\n", payload.length);

            sendMessage(message);
            sendMessage(payload);
            replications.add(clientSocket);
            //            try {
            //                Thread.sleep(50);
            //            } catch (InterruptedException e) {
            //                // TODO Auto-generated catch block
            //                e.printStackTrace();
            //            }
            //            sendMessage("*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n");
            return;
        }
        if (handshakeDone)
            return;
        sendMessage(message);
    }

    private boolean checkCommand(String[] commandWords, String givenCommand) {
        return checkCommand(commandWords, givenCommand, 0);
    }

    private boolean checkCommand(String[] commandWords, String givenCommand, int index) {
        return commandWords[index].toLowerCase().equals(givenCommand.toLowerCase());
    }

    private void sendMessage(String message) throws IOException {
        sendMessage(message.getBytes());

    }

    private void sendMessage(byte[] arr) throws IOException {
        clientSocket.getOutputStream().write(arr);
        clientSocket.getOutputStream().flush();
    }

    private String convertToMessage(Map<String, Object> keyValueResponse) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append("\r\n");
        keyValueResponse.forEach((k, v) -> {
            String line = k + ":" + v;
            stringBuilder.append(line + "\r\n");
        });
        return "$" + (stringBuilder.toString().length() - 4) + stringBuilder.toString();
    }

    private String parseCommand() throws IOException {
        byte wordValue = readByteFromSocket();
        byte[] buf = new byte[1024];

        int index = 0;
        while (true) {
            byte b = readByteFromSocket();
            buf[index] = b;
            try {
                if (buf[index - 1] == 13 && buf[index] == 10) {
                    break;
                }
            } catch (Exception e) {
                // TODO: handle exception
            }
            index++;
        }
        index -= 1; //remove /r/n
        byte[] shrinkedBuffer = new byte[index];
        System.arraycopy(buf, 0, shrinkedBuffer, 0, index);
        int wordLength = Integer.valueOf(new String(shrinkedBuffer));
        String command = new String(readNBytesFromSocket(wordLength));
        skipNewLine();
        return command;
    }

    private void skipNewLine() throws IOException {
        readNBytesFromSocket(2);
    }

    private String getStringValueOfByte(byte b) {
        return new String(new byte[] {b});
    }

    private byte readByteFromSocket() throws IOException {
        byte b = (byte) clientSocket.getInputStream().read();
        offset.incrementAndGet();
        return b;
    }

    private byte[] readNBytesFromSocket(int n) throws IOException {
        byte[] arr = clientSocket.getInputStream().readNBytes(n);
        offset.set(offset.get() + n);
        return arr;
    }
}
