import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class Configuration {

    private String replicaOf;
}
