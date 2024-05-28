import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class RedisStream {

    //time,id
    private Map<Long, List<Long>> stream = new ConcurrentHashMap<Long, List<Long>>();
    private long lastID = -1;
    private long lastIndex = -1;
    //time+index, Pair
    private Map<String, Pair> streamValues = new ConcurrentHashMap<String, Pair>();

    public String putMap(String id, String key, String value) throws IllegalArgumentException {
        if (id.equals("0-0")) {
            throw new IllegalArgumentException(
                            "ERR The ID specified in XADD must be greater than 0-0");
        }
        String[] idArr = id.split("-");
        Long ms = Long.parseLong(idArr[0]);
        Long index = null;
        if (!idArr[1].equals("*")) {
            index = Long.parseLong(idArr[1]);
        } else {
            index = lastIndex + 1;
        }
        List<Long> indexList = stream.get(index);

        if (!idArr[1].equals("*") && lastID >= Long.parseLong(id.replaceAll("-", ""))) {
            throw new IllegalArgumentException(
                            "ERR The ID specified in XADD is equal or smaller than the target stream top item");

        }

        if (indexList == null)
            indexList = new CopyOnWriteArrayList<Long>();
        indexList.add(index);
        stream.put(ms, indexList);
        lastID = ms + index;
        streamValues.put(ms + "-" + index, Pair.builder().key(key).value(value).build());
        lastIndex = index;
        return ms + "-" + index;

    }

}
