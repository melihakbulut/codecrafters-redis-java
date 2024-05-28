import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
                //                byte[] b = Files.readAllBytes(dbFile.toPath());
                InputStream is = new FileInputStream(dbFile);
                is.readNBytes(47);
                int index = 0;
                byte[] buf = new byte[1000];
                while (true) {
                    byte b = (byte) is.read();
                    if (b == -1)
                        break;
                    buf[index] = b;
                    index++;
                }
                byte[] shrinkedBuffer = new byte[index];
                System.arraycopy(buf, 0, shrinkedBuffer, 0, index);

                String command = new String(shrinkedBuffer);
                System.out.println(command);
                //                System.out.println(Arrays.toString(b));
                //                System.out.println(new String(b));
                //                putMap("pineapple", "pear");
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
