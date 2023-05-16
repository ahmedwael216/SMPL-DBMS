package DB;

import java.io.IOException;
import java.io.Serializable;

public class ClusteringKeySearch extends SearchStrategy{

    public static DBVector<Record> Search (SQLTerm query,String[] keys, Record prototype) throws IOException, ClassNotFoundException, CloneNotSupportedException {

        DBVector<Record> result = new DBVector<Record>();
        String tableName = query._strTableName;
        String colName = query._strColumnName;
        String operator = query._strOperator;

        int colIndex = getColIndex(keys, colName);

        int numberOfPages = TablePersistence.getNumberOfPagesForTable(tableName);

        int pageno = TablePersistence.findPageNumber(numberOfPages,tableName,query._strOperator);

        Page page = TablePersistence.deserialize(pageno,tableName);


        DBVector<Record> records = page.getRecords();

        Record record = (Record) prototype.clone();

        record.setItem(0, (Serializable) query._objValue);


        int recordno = records.binarySearch(record);


        switch (operator){
            case "=": ClusteringKeySearch.equal(pageno,recordno,tableName,result);
            case ">": ClusteringKeySearch.greaterThan(pageno,recordno,tableName,result);
            case ">=": ClusteringKeySearch.greaterThanOrEqual(pageno,recordno,tableName,result);
            case  "<": ClusteringKeySearch.lessThan(pageno,recordno,tableName,result);
            case "<=": ClusteringKeySearch.lessThanOrEqual(pageno,recordno,tableName,result);
            case "!=": ClusteringKeySearch.notEqual(pageno,recordno,tableName,result);
        }

        return result;
    }


    public static void greaterThan(int pageno, int recordno, String tableName,DBVector<Record> result) throws IOException, ClassNotFoundException {
        if(recordno>=0) recordno++;
        else recordno = -(recordno+1);

        int numberOfPages = TablePersistence.getNumberOfPagesForTable(tableName);

        for (;pageno<numberOfPages;pageno++){
            Page page = TablePersistence.deserialize(pageno,tableName);
            DBVector<Record> records = page.getRecords();
            for(;recordno<records.size();recordno++){
                result.add(records.get(recordno));
            }
            recordno = 0;
        }

    }

    public static void lessThan(int pageno, int recordno, String tableName,DBVector<Record> result) throws IOException, ClassNotFoundException {
        if(recordno>=0) recordno--;
        else recordno = -(recordno+2);

        if(recordno<0){
            pageno--;
            recordno= TablePersistence.deserialize(pageno,tableName).getRecords().size()-1;
        }

        for(int i=0;i<pageno;i++){
            Page page = TablePersistence.deserialize(i,tableName);
            DBVector<Record> records = page.getRecords();
            for(int j=0;j<recordno;j++){
                result.add(records.get(j));
            }
        }

    }

    public static void equal(int pageno, int recordno, String tableName, DBVector<Record> result) throws IOException, ClassNotFoundException {
        if(recordno<0) return;
        Page page = TablePersistence.deserialize(pageno,tableName);
        DBVector<Record> records = page.getRecords();
        result.add(records.get(recordno));


    }
    public static void notEqual(int pageno, int recordno, String tableName, DBVector<Record> result) throws IOException, ClassNotFoundException {
        ClusteringKeySearch.lessThan(pageno,recordno,tableName,result);
        ClusteringKeySearch.greaterThan(pageno,recordno,tableName,result);

    }

    public static void greaterThanOrEqual(int pageno, int recordno, String tableName, DBVector<Record> result) throws IOException, ClassNotFoundException {
        ClusteringKeySearch.equal(pageno,recordno,tableName,result);
        ClusteringKeySearch.greaterThan(pageno,recordno,tableName,result);
    }

    public static void lessThanOrEqual(int pageno, int recordno, String tableName, DBVector<Record> result) throws IOException, ClassNotFoundException {
        ClusteringKeySearch.equal(pageno,recordno,tableName,result);
        ClusteringKeySearch.lessThan(pageno,recordno,tableName,result);
    }




}
