package DB;

import java.beans.Expression;
import java.io.IOException;

public class LinearSearch extends   SearchStrategy{

    public static DBVector<Record> Search (SQLTerm query,String[] keys) throws IOException, ClassNotFoundException {

        DBVector<Record> result = new DBVector<Record>();
        String tableName = query._strTableName;
        String colName = query._strColumnName;
        String operator = query._strOperator;

        int colIndex = getColIndex(keys, colName);

        int numberOfPages = TablePersistence.getNumberOfPagesForTable(tableName);
        TablePersistence tp = new TablePersistence();
        for (int pageno=0;pageno<numberOfPages;pageno++){
            Page page = tp.deserialize(pageno,tableName);
            DBVector<Record> records = page.getRecords();
            for(Record record : records){
                if(expressionEval(operator, (Comparable) record.getItem(colIndex), (Comparable) query._objValue)) result.add(record);
            }

        }

        return result;
    }


}
