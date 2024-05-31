package com.redis;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@Builder
public class Configuration {

    private String replicaOf;
    private Integer port;
    private String dir;
    private String dbFileName;

}
