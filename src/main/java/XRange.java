import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString
@NoArgsConstructor
@Setter
public class XRange {

    private List<XRangeItem> xrangeItems = new ArrayList<XRangeItem>();
    private String streamKey;

    @Getter
    @AllArgsConstructor
    @ToString
    public static class XRangeItem {
        private String msIndex;
        private List<Pair> pairList = new ArrayList<Pair>();
    }
}
