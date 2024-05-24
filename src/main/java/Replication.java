import java.util.HashMap;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Replication {

    private Map<String, Object> keyValueMap = new HashMap<String, Object>();

    public Replication() {
        keyValueMap.put("role", "master");
        keyValueMap.put("connected_slaves", 0);
        keyValueMap.put("master_replid", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
        keyValueMap.put("master_repl_offset", 0);
        keyValueMap.put("second_repl_offset", -1);
        keyValueMap.put("repl_backlog_active", 0);
        keyValueMap.put("repl_backlog_size", 1048576);
        keyValueMap.put("repl_backlog_first_byte_offset", 0);
        keyValueMap.put("repl_backlog_histlen", "");
    }

}
