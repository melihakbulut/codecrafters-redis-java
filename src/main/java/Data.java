import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import lombok.Getter;

@Getter
public class Data {

    private Configuration configuration;

    public Pair parseAsPair(ByteBuffer buffer) {
        Long expiry = null;
        if (buffer.get() == -4) {
            expiry = buffer.order(ByteOrder.LITTLE_ENDIAN).getLong();
            buffer.get();
        }
        String key = null;
        String value = null;
        int keyLength = buffer.get();
        byte[] keyBuffer = new byte[keyLength];
        buffer.get(keyBuffer, 0, keyLength);
        key = new String(keyBuffer);

        int valueLength = buffer.get();
        byte[] valueBuffer = new byte[valueLength];
        buffer.get(valueBuffer, 0, valueLength);
        value = new String(valueBuffer);
        return Pair.builder().key(key).value(value).expiry(expiry).build();
    }

    public Data(Configuration configuration) {
        this.configuration = configuration;
        File dbFile = new File(configuration.getDir() + "/" + configuration.getDbFileName());
        if (dbFile.exists()) {
            System.out.println(dbFile.getAbsolutePath() + " file exists");
            try {
                byte[] buf = Files.readAllBytes(dbFile.toPath());
                System.out.println(Arrays.toString(buf));
                System.out.println(new String(buf));
                int index = 0;
                while (true) {
                    try {
                        if (buf[index - 3] == -5) {
                            int pairCount = buf[index - 2];
                            ByteBuffer byteBuffer = ByteBuffer.wrap(buf);
                            byteBuffer.position(index);
                            for (int i = 0; i < pairCount; i++) {
                                Pair pair = parseAsPair(byteBuffer);

                                if (pair.getExpiry() != null) {
                                    long expiryValue = pair.getExpiry()
                                                       - System.currentTimeMillis();
                                    System.out.println("parse pair " + pair + " expiry : "
                                                       + expiryValue);
                                    putMap(pair.getKey(), pair.getValue(), expiryValue);
                                } else
                                    putMap(pair.getKey(), pair.getValue());
                            }

                            break;

                        }
                    } catch (Exception e) {

                    }
                    index++;
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private Map<String, String> keyValueMap = new HashMap<String, String>();
    private Map<String, Long> keyEntryTimeMap = new HashMap<String, Long>();

    public void putMap(String key, String value, Long timeoutMs) {
        long expiryTime = -1;
        if (timeoutMs > 0)
            expiryTime = System.currentTimeMillis() + timeoutMs;
        keyValueMap.put(key, value);
        keyEntryTimeMap.put(key, expiryTime);
    }

    public void putMap(String key, String value) {
        putMap(key, value, -1l);
    }

    public String getFromMap(String key) throws Exception {

        System.out.println(keyEntryTimeMap.get(key));
        System.out.println(System.currentTimeMillis());
        if (keyEntryTimeMap.get(key) != -1
            && System.currentTimeMillis() > keyEntryTimeMap.get(key)) {
            throw new TimeoutException();
        }
        return keyValueMap.get(key);

    }
}
