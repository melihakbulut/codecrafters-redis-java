import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import lombok.Getter;

@Getter
public class Data {

    private Configuration configuration;

    public Data(Configuration configuration) {
        this.configuration = configuration;
        File dbFile = new File(configuration.getDir() + "/" + configuration.getDbFileName());
        if (dbFile.exists()) {
            System.out.println(dbFile.getAbsolutePath() + " file exists");
            try {
                String key = null;
                String value = null;
                byte[] buf = Files.readAllBytes(dbFile.toPath());
                System.out.println(Arrays.toString(buf));
                int index = 0;
                while (true) {
                    try {
                        if (buf[index - 3] == -5 && buf[index - 2] == 1 && buf[index - 1] == 0
                            && buf[index] == 0) {
                            int keyLength = buf[++index];
                            byte[] keyBuffer = new byte[keyLength];
                            System.arraycopy(buf, index + 1, keyBuffer, 0, keyLength);
                            key = new String(keyBuffer);
                            index += keyLength;

                            int valueLength = buf[++index];
                            byte[] valueBuffer = new byte[valueLength];
                            System.arraycopy(buf, index + 1, valueBuffer, 0, valueLength);
                            value = new String(valueBuffer);
                            index += valueLength;
                            break;

                        }
                    } catch (Exception e) {

                    }

                    index++;
                }

                putMap(key, value);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private String getStringValueOfByte(byte b) {
        return new String(new byte[] {b});
    }

    public String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for (byte b : a)
            sb.append(String.format("%02x", b));
        return sb.toString();
    }

    public static byte[] hexStringToByteArray(String hex) {
        int l = hex.length();
        byte[] data = new byte[l / 2];
        for (int i = 0; i < l; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                  + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
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
