import java.io.IOException;
import java.net.Socket;

public class RedisHandler implements Runnable {

    private Socket clientSocket;

    public RedisHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try {
            clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
            Thread.sleep(100);
            clientSocket.getOutputStream().write("+PONG\r\n".getBytes());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void listen(Socket clientSocket) throws IOException {
        final int bufLen = 128 * 0x400; // 4KB
        byte[] buf = new byte[bufLen];
        int index = 0;
        while (true) {
            byte b = (byte) clientSocket.getInputStream().read();
            if (b == -1)
                break;
            buf[index] = b;

            index++;
        }

    }

}
