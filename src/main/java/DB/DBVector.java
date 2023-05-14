package DB;

import java.io.Serializable;
import java.util.Vector;

public class DBVector<T> extends Vector<T> implements Cloneable, Serializable {
    public Object clone() {
        return super.clone();
    }

    public int binarySearch(T record) {
        int low = 0;
        int high = this.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            T midVal = this.get(mid);
            int cmp = ((Comparable) midVal).compareTo(record);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }

        return -(low + 1);
    }

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        for (T record : this) {
            res.append(record).append(" ");
        }
        return res.toString();
    }
}
