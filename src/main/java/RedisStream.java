import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RedisStream {

    //time,id
    private Map<Long, Long> stream = new ConcurrentHashMap<Long, Long>();
    //time+index, Pair
    private Map<String, Pair> streamValues = new ConcurrentHashMap<String, Pair>();

    public void putMap(String id, String key, String value) {
        String[] idArr = id.split("-");
        Long ms = Long.parseLong(idArr[0]);
        Long index = Long.parseLong(idArr[1]);
        stream.put(ms, index);
        streamValues.put(id, Pair.builder().key(key).value(value).build());
    }
}
