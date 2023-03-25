package DB;

import java.io.Serializable;

public class Page implements Serializable {
    DBVector<Record> records;

    public Page() {
        records = new DBVector<Record>();
    }

    public DBVector<Record> getRecords() {
        return records;
    }

    public Record insertRecord(Record record) throws DBAppException {
        int index = records.binarySearch(records, record);
        if (index < 0) {
            index = -index - 1;
            records.add(index, record);
            int maxRecordsCountPage = DbApp.maxRecordsCountPage;
            if (records.size() > maxRecordsCountPage) {
                return records.remove(maxRecordsCountPage);
            }
        } else {
            throw new DBAppException("Record already exists!");
        }
        return null;
    }

    public void updateRecord(Record oldRecord, Record newRecord) throws DBAppException {
        int index = records.binarySearch(records, oldRecord);
        if (index >= 0) {
            records.set(index, newRecord);
        } else {
            throw new DBAppException("Record not found!");
        }
    }

    public void deleteRecord(Record record) throws DBAppException {
        int index = records.binarySearch(records, record);
        if (index >= 0) {
            records.remove(index);
        } else {
            throw new DBAppException("Record not found!");
        }
    }

}
