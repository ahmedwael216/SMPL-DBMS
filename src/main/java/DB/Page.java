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
            if (records.size() > maxRecordsCountPage) {
                return records.remove(maxRecordsCountPage);
            }
            updateMinMax();
        } else {
            throw new DBAppException("Record already exists!");
        }
        return null;
    }

    public void updateRecord(Record newRecord) throws DBAppException {
        int index = records.binarySearch(newRecord);
        if (index >= 0) {
            records.set(index, newRecord);
            updateMinMax();
        } else {
            throw new DBAppException("Record not found!");
        }
    }

    public void deleteRecord(Record record) throws DBAppException {
        int index = records.binarySearch(record);
        if (index >= 0) {
            records.remove(index);
            updateMinMax();
        } else {
            throw new DBAppException("Record not found!");
        }
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
        return records.toString();
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }
}
