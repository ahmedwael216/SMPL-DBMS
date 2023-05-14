package DB;

import java.io.IOException;
import java.io.Serializable;

public class ClusteringKeySearch extends SearchStrategy{

    public static DBVector<Record> Search (SQLTerm[] queries,String[] keys,Record prototype) throws IOException, ClassNotFoundException, CloneNotSupportedException {

        DBVector<Record> result = new DBVector<Record>();
        SQLTerm query = queries[0];
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


        return result;
    }


    public static void greaterThan(int pageno, int recordno, String tableName,DBVector<Record> result){
        
    }

    public static void lessThan(int pageno, int recordno, String tableName,DBVector<Record> result){

    }

    public static void equal(int pageno, int recordno, String tablename, DBVector<Record> result){

    }
    public static void notEqual(int pageno, int recordno, String tablename, DBVector<Record> result){

    }

    public static void greaterThanOrEqual(int pageno, int recordno, String tablename, DBVector<Record> result){

    }

    public static void lessThanOrEqual(int pageno, int recordno, String tablename, DBVector<Record> result){

    }




}
