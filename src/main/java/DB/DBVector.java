package DB;

import java.util.Vector;

public class DBVector<T> extends Vector<T> implements Cloneable {
    public Object clone() {
        return super.clone();
    }

    public int binarySearch(DBVector<T> records, T record) {
        int low = 0;
        int high = records.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            T midVal = records.get(mid);
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
}
