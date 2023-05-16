package DB;

import java.io.Serializable;
import java.util.Date;

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
            int maxRecordsCountPage = DBApp.maxRecordsCountPage;
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

        // this is update record of course the records won't be equal
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

        if (index < 0 || !records.get(index).equals(record)) {
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
    public String printWithLength(int[] max) {
        StringBuilder sb =new StringBuilder();
        for( Record r: records){
            for (int i=0;i<r.getDBVector().size();i++){
                Object o= r.getDBVector().get(i);
                StringBuilder element;
                if(o!=null){
                    switch (o.getClass().getSimpleName()){
                        case "Integer":
                            element = new StringBuilder((Integer)o +"");
                            break;
                        case "Double":
                            element = new StringBuilder((Double)o +"");
                            break;
                        case "Date":
                            element = new StringBuilder((Date)o +"");
                            break;
                        default:
                            element = new StringBuilder((String)o);
                            break;
                    }
                    while(element.length()<max[i]){
                        element.append(" ");
                    }
                }else{
                    element = new StringBuilder("null");
                }
                sb.append(element);
                if(i!=r.getDBVector().size()-1){
                    sb.append("│");
                }
            }
            sb.append("\n");
        }
        return sb.toString();
    }
}
