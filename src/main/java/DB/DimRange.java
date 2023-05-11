package DB;

import java.util.Date;

public class DimRange {
    Comparable min;
    Comparable max;

    public DimRange(Comparable min, Comparable max) {
        this.min = min;
        this.max = max;
    }

    public DimRange[] split() {
        return null;
    }

}