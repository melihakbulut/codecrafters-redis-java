import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RedisHandler implements Runnable {

    private Socket clientSocket;

    private Map<String, String> keyValueMap = new HashMap<String, String>();

    public RedisHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
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
        if (commandWords[0].toLowerCase().equals("ping")) {
            message = "+PONG\r\n";
        } else if (commandWords[0].toLowerCase().equals("echo")) {
            message = String.format("$%s\r\n%s\r\n", commandWords[1].length(), commandWords[1]);

        } else if (commandWords[0].toLowerCase().equals("set")) {
            keyValueMap.put(commandWords[1], commandWords[2]);
            message = "+OK\r\n";
        } else if (commandWords[0].toLowerCase().equals("get")) {
            String value = keyValueMap.get(commandWords[1]);
            message = String.format("$%s\r\n%s\r\n", value.length(), value);
        }
        clientSocket.getOutputStream().write(message.getBytes());
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
        //        skipNewLine();
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
