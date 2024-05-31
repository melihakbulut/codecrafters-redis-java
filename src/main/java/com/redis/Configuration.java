package com.redis;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class Configuration {

    private String replicaOf;
    private Integer port;
    private String dir;
    private String dbFileName;

    private static Configuration configuration = null;

    public Configuration(String replicaOf, Integer port, String dir, String dbFileName) {
        configuration = new Configuration(replicaOf, port, dir, dbFileName);
    }

    public static Configuration getInstance() {
        return configuration;
    }

}
