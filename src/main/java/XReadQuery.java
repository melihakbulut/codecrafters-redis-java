import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

@Getter
@Builder
@ToString
public class XReadQuery {

    private String streamKey;
    private String fromMs;
    private String toMs;
}
