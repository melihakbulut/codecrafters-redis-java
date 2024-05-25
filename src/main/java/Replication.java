import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
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
            String message = "*1\r\n$4\r\nPING\r\n";
            socket.getOutputStream().write(message.getBytes());

        }
    }

}
