package com.redis;

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

    private static Data data;

    public static Data getData() {
        if (data == null) {
            data = new Data();
        }
        return data;
    }

    public Pair parseAsPair(ByteBuffer buffer) {
        Long expiry = null;
        if (buffer.get() == -4) {
            expiry = buffer.getLong();
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

    public Data() {
        File dbFile = new File(Main.getConfiguration().getDir() + "/"
                               + Main.getConfiguration().getDbFileName());
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
                            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
                            byteBuffer.position(index);
                            for (int i = 0; i < pairCount; i++) {
                                Pair pair = parseAsPair(byteBuffer);
                                if (pair.getExpiry() != null) {
                                    putMapWithEpoch(pair.getKey(), pair.getValue(),
                                                    pair.getExpiry());
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

    private Map<String, Object> keyValueMap = new HashMap<String, Object>();
    private Map<String, Long> keyEntryTimeMap = new HashMap<String, Long>();

    public void putMap(String key, String value, Long timeoutMs) {
        long expiryTime = -1;
        if (timeoutMs > 0)
            expiryTime = System.currentTimeMillis() + timeoutMs;
        keyValueMap.put(key, value);
        keyEntryTimeMap.put(key, expiryTime);
    }

    public void putMapWithEpoch(String key, String value, Long expiryTime) {
        keyValueMap.put(key, value);
        keyEntryTimeMap.put(key, expiryTime);
    }

    public void putMap(String key, String value) {
        putMap(key, value, -1l);
    }

    public Object getFromMap(String key) throws Exception {

        System.out.println(keyEntryTimeMap.get(key));
        System.out.println(System.currentTimeMillis());
        if (keyEntryTimeMap.get(key) != -1
            && System.currentTimeMillis() > keyEntryTimeMap.get(key)) {
            throw new TimeoutException();
        }
        return keyValueMap.get(key);

    }
}
