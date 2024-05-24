import java.io.IOException;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

public class RedisHandler implements Runnable {

    private Socket clientSocket;
    private Deque<String> commandQueue = new ArrayDeque<String>();

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

        }
        clientSocket.getOutputStream().write(message.getBytes());
    }

    private String parseCommand() throws IOException {
        byte wordValue = (byte) clientSocket.getInputStream().read();
        int wordLength = Integer
                        .valueOf(getStringValueOfByte((byte) clientSocket.getInputStream().read()));
        skipNewLine();
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
