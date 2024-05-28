import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class RedisStream {

    //time,id
    private Map<Long, List<Long>> stream = new ConcurrentHashMap<Long, List<Long>>();
    //time+index, Pair
    private Map<String, Pair> streamValues = new ConcurrentHashMap<String, Pair>();

    public String putMap(String id, String key, String value) throws IllegalArgumentException {
        if (id.equals("0-0")) {
            throw new IllegalArgumentException(
                            "ERR The ID specified in XADD must be greater than 0-0");
        }
        String[] idArr = id.split("-");
        Long ms = Long.parseLong(idArr[0]);
        long lastIndex = 0;
        List<Long> indexes = stream.get(ms);
        if (indexes == null) {
            indexes = new CopyOnWriteArrayList<Long>();
            stream.put(ms, indexes);
        }
        if (!indexes.isEmpty())
            lastIndex = indexes.get(indexes.size() - 1);
        else {
            if (ms == 0)
                lastIndex = 0;
            else
                lastIndex = -1;
        }
        boolean autoInc = idArr[1].equals("*");

        if (!autoInc) {
            Long sentIndex = Long.parseLong(idArr[1]);

            if (sentIndex <= lastIndex) {
                throw new IllegalArgumentException(
                                "ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }

        }

        Long nextIndex = lastIndex + 1;
        indexes.add(nextIndex);
        stream.put(ms, indexes);
        streamValues.put(ms + "-" + nextIndex, Pair.builder().key(key).value(value).build());
        return ms + "-" + nextIndex;

    }

}
