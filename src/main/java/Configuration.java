import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Builder
@Getter
@ToString
public class Configuration {

    private String replicaOf;
    private Integer port;
    private String dir;
    private String dbFileName;
}
