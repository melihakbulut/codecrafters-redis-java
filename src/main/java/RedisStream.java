import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class RedisStream {

    //time,id
    private Map<Long, List<Long>> stream = new ConcurrentHashMap<Long, List<Long>>();
    //time+index, Pair
    private Map<String, List<Pair>> streamValues = new ConcurrentHashMap<String, List<Pair>>();

    public String putMap(String id, String[] keyValues) throws IllegalArgumentException {
        if (id.equals("0-0")) {
            throw new IllegalArgumentException(
                            "ERR The ID specified in XADD must be greater than 0-0");
        }
        String[] idArr = id.split("-");
        boolean autoIncWhole = idArr[0].equals("*");
        Long ms = null;
        if (autoIncWhole)
            ms = System.currentTimeMillis();
        else
            ms = Long.parseLong(idArr[0]);

        for (Long msValues : stream.keySet()) {
            if (ms < msValues)
                throw new IllegalArgumentException(
                                "ERR The ID specified in XADD is equal or smaller than the target stream top item");
        }

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
        boolean autoInc = autoIncWhole ? true : idArr[1].equals("*");

        if (autoInc) {
            lastIndex++;
        } else {
            Long sentIndex = Long.parseLong(idArr[1]);

            if (sentIndex <= lastIndex) {
                throw new IllegalArgumentException(
                                "ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
            lastIndex = sentIndex;
        }

        Long nextIndex = lastIndex;
        indexes.add(nextIndex);
        stream.put(ms, indexes);
        String msIndex = ms + "-" + nextIndex;
        List<Pair> pairList = streamValues.get(msIndex);
        if (pairList == null) {
            pairList = new CopyOnWriteArrayList<Pair>();
        }

        for (int i = 3; i < keyValues.length; i += 2) {
            pairList.add(Pair.builder().key(keyValues[i]).value(keyValues[i + 1]).build());
        }

        streamValues.put(msIndex, pairList);
        return msIndex;

    }

    public XRange getBetweenFromMs(String fromMs, String toMs) throws IllegalArgumentException {
        Map<String, List<Pair>> subSetStreamValues = new ConcurrentHashMap<String, List<Pair>>();

        Long fromMsLong = null;
        Long toMsLong = null;
        boolean fromBeginning = fromMs.equals("-");

        if (fromMs.contains("-") && !fromBeginning) {
            if (fromMs.split("-")[0].equals("0")) {
                fromMsLong = Long.parseLong(fromMs.split("-")[1]);
                toMsLong = Long.parseLong(toMs.split("-")[1]);
            } else {
                fromMsLong = Long.parseLong(fromMs.replace("-", ""));
                toMsLong = Long.parseLong(toMs.replace("-", ""));
            }
        } else {
            if (fromMs.equals("0")) {
                fromMsLong = Long.parseLong(fromMs.split("-")[1]);
                toMsLong = Long.parseLong(toMs.split("-")[1]);
            } else {
                fromMsLong = fromBeginning ? -1 : Long.parseLong(fromMs.replace("-", ""));
                toMsLong = Long.parseLong(toMs.replace("-", ""));
            }
        }
        System.out.println(String.format("fromMsLong %s, toMsLong %s", fromMsLong, toMsLong));

        for (Map.Entry<String, List<Pair>> streamValuesItem : streamValues.entrySet()) {
            if (streamValuesItem.getKey().split("-")[0].equals("0")) {
                long ms = Long.parseLong(streamValuesItem.getKey().split("-")[1]);
                if (ms >= fromMsLong && ms <= toMsLong) {
                    subSetStreamValues.put(streamValuesItem.getKey(), streamValuesItem.getValue());
                }
            } else {
                long ms = Long.parseLong(streamValuesItem.getKey().replace("-", ""));
                if (fromMsLong >= ms && ms <= toMsLong) {
                    subSetStreamValues.put(streamValuesItem.getKey(), streamValuesItem.getValue());
                }
            }
        }
        XRange xRange = new XRange();
        subSetStreamValues.forEach((k, v) -> xRange.getXrangeItems()
                        .add(new XRange.XRangeItem(k, v)));

        Collections.sort(xRange.getXrangeItems(), new Comparator<XRange.XRangeItem>() {
            @Override
            public int compare(XRange.XRangeItem o1, XRange.XRangeItem o2) {
                return o1.getMsIndex().compareTo(o2.getMsIndex());
            }
        });
        System.out.println(xRange);
        return xRange;
    }
}
