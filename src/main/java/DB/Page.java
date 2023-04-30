package DB;

import java.io.Serializable;

public class Page implements Serializable, Comparable {
    DBVector<Record> records;
    Comparable minValue;
    Comparable maxValue;

    public Page() {
        records = new DBVector<Record>();
    }

    public DBVector<Record> getRecords() {
        return records;
    }

    public Record insertRecord(Record record) throws DBAppException {
        int index = records.binarySearch(record);
        if (index < 0) {
            index = -index - 1;
            records.add(index, record);
            int maxRecordsCountPage = DbApp.maxRecordsCountPage;
            Record lastRecord = null;
            if (records.size() > maxRecordsCountPage) {
                lastRecord = records.remove(maxRecordsCountPage);
            }
            updateMinMax();
            return lastRecord;
        } else {
            throw new DBAppException("Record already exists!");
        }
    }

    public void updateRecord(Record newRecord) throws DBAppException {
        int index = records.binarySearch(newRecord);
        if(!records.get(index).equals(newRecord)){
            throw new DBAppException("Record not found!");
        }
        if (index >= 0) {
            records.set(index, newRecord);
            updateMinMax();
        } else {
            throw new DBAppException("Record not found!");
        }
    }

    public int deleteLinear(Record r) {
        int cntDel = 0;
        for (int i = records.size() - 1; i >= 0; i--) {
            if (records.get(i).equals(r)) {
                records.remove(i);
                cntDel++;
            }
        }
        if (!records.isEmpty())
            updateMinMax();
        return cntDel;
    }

    public int deleteRecord(Record record) throws DBAppException {
        int index = records.binarySearch(record);
        if(!records.get(index).equals(record)){
            throw new DBAppException("Record not found!");
        }
        if (index >= 0) {
            records.remove(index);
            if (!records.isEmpty())
                updateMinMax();
        } else {
            throw new DBAppException("Record not found!");
        }
        return 1;
    }

    public void updateMinMax() {
        this.minValue = (Comparable) this.records.get(0).getPrimaryKey();
        this.maxValue = (Comparable) this.records.get(records.size() - 1).getPrimaryKey();
    }

    @Override
    public int compareTo(Object o) {
        if (((Comparable) o).compareTo(maxValue) <= 0 && ((Comparable) o).compareTo(minValue) >= 0)
            return 0;
        if (((Comparable) o).compareTo(maxValue) > 0) {
            return 1;
        }

        return -1;
    }

    public String toString() {
        return records.toString() + " " + minValue + " " + maxValue;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }
}
