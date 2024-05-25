import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Replication {

    private Configuration configuration;
    private Map<String, Object> keyValueMap = new HashMap<String, Object>();

    public Replication(Configuration configuration, String role) {
        this.configuration = configuration;
        keyValueMap.put("role", role);
        //        keyValueMap.put("connected_slaves", 0);
        keyValueMap.put("master_replid", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
        keyValueMap.put("master_repl_offset", 0);
        //        keyValueMap.put("second_repl_offset", -1);
        //        keyValueMap.put("repl_backlog_active", 0);
        //        keyValueMap.put("repl_backlog_size", 1048576);
        //        keyValueMap.put("repl_backlog_first_byte_offset", 0);
        //        keyValueMap.put("repl_backlog_histlen", "");
        try {
            sendMasterPing();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void sendMasterPing() throws UnknownHostException, IOException {
        if (getKeyValueMap().get("role").equals("slave")) {
            String[] arr = configuration.getReplicaOf().split("\\s+");
            String ip = arr[0];
            Integer port = Integer.valueOf(arr[1]);
            Socket socket = new Socket(ip, port);
            System.out.println(String.format("connectin master node %s:%s", ip, port));
            String ping = "*1\r\n$4\r\nPING\r\n";
            sendMessage(socket, ping);
            handle("ping", waitForResponse(socket));
            String replConf = String
                            .format("*3\r\n$8\r\nREPLCONF\\r\n$14\r\nlistening-port\r\n$%s\r\n%s\r\n",
                                    port.toString().length(), port);
            sendMessage(socket, replConf);
            handle("replconf", waitForResponse(socket));
            String replConfSecond = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
            sendMessage(socket, replConfSecond);
            handle("replconf2", waitForResponse(socket));
        }
    }

    private void handle(String currentOperation, String[] commandWords) throws IOException {
        System.out.println(Arrays.toString(commandWords));
        if (currentOperation.equals("ping") && !commandWords[0].equals("PONG")) {
            throw new RuntimeException();
        } else if (currentOperation.equals("replconf") && !commandWords[0].equals("OK")) {
            throw new RuntimeException();
        } else if (currentOperation.equals("replconf2") && !commandWords[0].equals("OK")) {
            throw new RuntimeException();
        }
    }

    private void sendMessage(Socket socket, String message) throws IOException {
        socket.getOutputStream().write(message.getBytes());
        socket.getOutputStream().flush();
    }

    public String[] waitForResponse(Socket socket) throws IOException {
        String[] returnedMessage = null;
        while (true) {
            //            System.out.print((byte) clientSocket.getInputStream().read() + ",");
            byte b = (byte) socket.getInputStream().read();
            if (b == '*') {
                int commandWordLength = Integer.valueOf(getStringValueOfByte((byte) socket
                                .getInputStream().read()));
                skipNewLine(socket);
                String[] commandWords = new String[commandWordLength];
                for (int i = 0; i < commandWordLength; i++) {
                    commandWords[i] = parseCommand(socket);
                }
                System.out.println(Arrays.toString(commandWords));
                returnedMessage = commandWords;
                break;
            }
        }
        return returnedMessage;
    }

    private void skipNewLine(Socket socket) throws IOException {
        socket.getInputStream().readNBytes(2);
    }

    private String getStringValueOfByte(byte b) {
        return new String(new byte[] {b});
    }

    private String parseCommand(Socket socket) throws IOException {
        byte wordValue = (byte) socket.getInputStream().read();
        byte[] buf = new byte[1024];

        int index = 0;
        while (true) {
            byte b = (byte) socket.getInputStream().read();
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
        String command = new String(socket.getInputStream().readNBytes(wordLength));
        skipNewLine(socket);
        return command;
    }
}
