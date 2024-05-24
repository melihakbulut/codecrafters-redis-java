import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Replication {

    private Map<String, String> keyValueMap = new HashMap<String, String>();

    public Replication() {
        keyValueMap.put("role", "master");
        keyValueMap.put("connected_slaves", "");
        keyValueMap.put("master_replid", "");
        keyValueMap.put("master_repl_offset", "");
        keyValueMap.put("second_repl_offset", "");
        keyValueMap.put("repl_backlog_active", "");
        keyValueMap.put("repl_backlog_size", "");
        keyValueMap.put("repl_backlog_first_byte_offset", "");
        keyValueMap.put("repl_backlog_histlen", "");

        //        :master
        //        :0
        //        :8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
        //        :0
        //        :-1
        //        :0
        //        :1048576
        //        :0
        //        :
    }

}
