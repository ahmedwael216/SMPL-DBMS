package DB;

import java.io.Serializable;
import java.util.Vector;

public class Page implements Serializable {
    Vector<Record> records;

    public Page() {
        records = new Vector<Record>();
    }

    public Vector<Record> getRecords() {
        return records;
    }

    public static void main(String[] args) {
        int maxRecordsCountinPage = DbApp.maxRecordsCountinPage;
        System.out.println(maxRecordsCountinPage);
    }

    // insert sorted => binary search inside the page => return null if inserted => return the last record if page is full
    
}
