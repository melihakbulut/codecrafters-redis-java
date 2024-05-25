import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RedisHandler implements Runnable {

    private Socket clientSocket;

    private Map<String, String> keyValueMap = new HashMap<String, String>();
    private Map<String, Long> keyEntryTimeMap = new HashMap<String, Long>();

    private static final List<Socket> replications = new ArrayList<Socket>();

    private Replication replication;
    private Configuration configuration;

    private static final String notFound = "$-1\r\n";

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
            //            System.out.print((byte) clientSocket.getInputStream().read() + ",");
            byte b = (byte) clientSocket.getInputStream().read();
            if (b == '*') {
                int commandWordLength = Integer.valueOf(getStringValueOfByte((byte) clientSocket
                                .getInputStream().read()));
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
            message = "+OK\r\n";
            if (commandWords.length == 3) {
                String host = ((InetSocketAddress) clientSocket.getRemoteSocketAddress())
                                .getHostName();
                Integer port = Integer.valueOf(commandWords[2]);
                new Thread(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            Thread.sleep(500);
                            replications.add(new Socket(host, port));
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }

                    }
                }).start();
                ;
            }
        } else if (checkCommand(commandWords, "echo")) {

            message = String.format("$%s\r\n%s\r\n", commandWords[1].length(), commandWords[1]);

        } else if (checkCommand(commandWords, "set")) {
            if (commandWords.length > 3)
                putMap(commandWords[1], commandWords[2], Long.parseLong(commandWords[4]));
            else
                putMap(commandWords[1], commandWords[2]);

            message = "+OK\r\n";
            String key = commandWords[1];
            String value = commandWords[2];
            String setFormat = "*3\r\n$3\r\nSET\r\n$%s\r\n%s\r\n$%s\r\n%s\r\n";
            String replicaMessage = String.format(setFormat, key.length(), key, value.length(),
                                                  value);
            for (Socket replica : replications) {
                System.out.println("sent to replica " + replicaMessage);
                replica.getOutputStream().write(replicaMessage.getBytes());
            }

        } else if (checkCommand(commandWords, "get")) {
            try {
                String value = getFromMap(commandWords[1]);
                message = String.format("$%s\r\n%s\r\n", value.length(), value);
            } catch (TimeoutException e) {
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
            //            replications.add(clientSocket);
            return;
        }

        sendMessage(message);
    }

    private boolean checkCommand(String[] commandWords, String givenCommand) {
        return checkCommand(commandWords, givenCommand, 0);
    }

    private boolean checkCommand(String[] commandWords, String givenCommand, int index) {
        return commandWords[index].toLowerCase().equals(givenCommand.toLowerCase());
    }

    private void putMap(String key, String value) {
        putMap(key, value, -1l);
    }

    private void putMap(String key, String value, Long timeoutMs) {
        long expiryTime = -1;
        if (timeoutMs > 0)
            expiryTime = System.currentTimeMillis() + timeoutMs;
        keyValueMap.put(key, value);
        keyEntryTimeMap.put(key, expiryTime);
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

    private String getFromMap(String key) throws TimeoutException {
        System.out.println(keyEntryTimeMap.get(key));
        System.out.println(System.currentTimeMillis());
        if (keyEntryTimeMap.get(key) != -1
            && System.currentTimeMillis() > keyEntryTimeMap.get(key)) {
            throw new TimeoutException();
        }
        return keyValueMap.get(key);
    }

    private String parseCommand() throws IOException {
        byte wordValue = (byte) clientSocket.getInputStream().read();
        byte[] buf = new byte[1024];

        int index = 0;
        while (true) {
            byte b = (byte) clientSocket.getInputStream().read();
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
        String command = new String(clientSocket.getInputStream().readNBytes(wordLength));
        skipNewLine();
        return command;
    }

    private void skipNewLine() throws IOException {
        clientSocket.getInputStream().readNBytes(2);
    }

    private String getStringValueOfByte(byte b) {
        return new String(new byte[] {b});
    }
}
