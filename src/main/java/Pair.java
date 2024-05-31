import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class Pair {

    private String key;
    private String value;
    private Long expiry;
}
