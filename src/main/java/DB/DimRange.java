package DB;

import java.util.Date;
import java.io.Serializable;
import java.util.*;

public class DimRange implements Serializable {
    private Comparable min;
    private Comparable max;

    public DimRange(Comparable min, Comparable max) {
        this.min = min;
        this.max = max;
    }

    public DimRange[] split() {
        if (min instanceof Integer) {
            return splitInteger((int) min, (int) max);
        } else if (min instanceof String) {
            return splitString((String) min, (String) max, Math.min(((String) min).length(), ((String) max).length()));
        } else if (min instanceof Date) {
            return splitDate((Date) min, (Date) max);
        } else {
            return splitDouble((double) min, (double) max);
        }
    }

    public DimRange[] splitInteger(int min, int max) {
        int mid = (min + max) / 2;
        DimRange[] resRanges = new DimRange[2];
        resRanges[0] = new DimRange(min, mid);
        resRanges[1] = new DimRange(mid, max);
        return resRanges;
    }

    static DimRange[] splitString(String S, String T, int N) {
        // Stores the base 26 digits after addition
        int[] a1 = new int[N + 1];

        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int) S.charAt(i) - 97
                    + (int) T.charAt(i) - 97;
        }

        // Iterate from right to left
        // and add carry to next position
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int) a1[i] / 26;
            a1[i] %= 26;
        }

        // Reduce the number to find the middle
        // string by dividing each position by 2
        for (int i = 0; i <= N; i++) {

            // If current value is odd,
            // carry 26 to the next index value
            if ((a1[i] & 1) != 0) {

                if (i + 1 <= N) {
                    a1[i + 1] += 26;
                }
            }

            a1[i] = (int) a1[i] / 2;
        }

        StringBuilder res = new StringBuilder();
        for (int i = 1; i <= N; i++) {
            res.append((char) (a1[i] + 97));
        }

        String mid = res.toString();
        DimRange[] resRanges = new DimRange[2];
        resRanges[0] = new DimRange(S, mid);
        resRanges[1] = new DimRange(mid, T);
        return resRanges;
    }

    public DimRange[] splitDate(Date min, Date max) {
        long mid = (min.getTime() + max.getTime()) / 2;
        DimRange[] resRanges = new DimRange[2];
        resRanges[0] = new DimRange(min, new Date(mid));
        resRanges[1] = new DimRange(new Date(mid), max);
        return resRanges;
    }

    public DimRange[] splitDouble(double min, double max) {
        double mid = (min + max) / 2;
        DimRange[] resRanges = new DimRange[2];
        resRanges[0] = new DimRange(min, mid);
        resRanges[1] = new DimRange(mid, max);
        return resRanges;
    }

    public boolean inRange(Comparable value) {
        return min.compareTo(value) <= 0 && max.compareTo(value) >= 0;
    }

    public boolean equals(DimRange other) {
        return min.equals(other.min) && max.equals(other.max);
    }

    public Comparable getMin() {
        return min;
    }

    public Comparable getMax() {
        return max;
    }

    public String toString() {
        return min + " " + max;
    }

    public boolean intersect(DimRange other, boolean includeL, boolean includeR) {
        if ((includeR ? min.compareTo(other.max) > 0 : min.compareTo(other.max) >= 0)
                || (includeL ? max.compareTo(other.min) < 0 : max.compareTo(other.min) <= 0)) {
            return false;
        }
        return true;
    }
}