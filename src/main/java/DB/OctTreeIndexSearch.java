package DB;

import java.io.IOException;
import java.text.ParseException;

public class OctTreeIndexSearch extends SearchStrategy{
    public static DBVector<Record> Search (SQLTerm[] queries, String[] keys, Node indexRoot) throws DBAppException, IOException, ParseException, ClassNotFoundException {
        SQLTerm firstQuery = queries[0];
        SQLTerm secondQuery = queries[0];
        SQLTerm thirdQuery = queries[0];

        Comparable minX = null, maxX=null, minY=null, maxY=null, minZ=null, maxZ=null;
        boolean[] includeLRX = setMinMaxQuery(firstQuery, minX, maxX);
        boolean[] includeLRY = setMinMaxQuery(secondQuery, minY, maxY);
        boolean[] includeLRZ = setMinMaxQuery(thirdQuery, minZ, maxZ);

        Table table = DBApp.getTable(firstQuery._strTableName);

        DimRange xRange = new DimRange(minX, maxX);
        DimRange yRange = new DimRange(minY, maxY);
        DimRange zRange = new DimRange(minZ, maxZ);

        DBVector<Integer> pageIndices = indexRoot.search(xRange, yRange, zRange, includeLRX[0], includeLRY[0], includeLRZ[0], includeLRX[1],includeLRY[1], includeLRZ[1]);

        DBVector<Record> searchResult = getPageRecords(pageIndices, firstQuery._strTableName, queries,keys);

        return searchResult;
    }

    public static  DBVector<Record> getPageRecords(DBVector<Integer> pageIndeces, String strTableName, SQLTerm[] queries, String[] keys) throws IOException, ClassNotFoundException {

        DBVector<Record> result = new DBVector<>();

        for(int x : pageIndeces) {
            Page p = TablePersistence.deserialize(x,strTableName);
            DBVector<Record> records = p.getRecords();
            for(Record record : records){
                if(expressionEval(queries[0]._strOperator, (Comparable) record.getItem(getColIndex(keys,queries[0]._strColumnName)), (Comparable) queries[0]._objValue) &&
                        expressionEval(queries[1]._strOperator, (Comparable) record.getItem(getColIndex(keys,queries[1]._strColumnName)), (Comparable) queries[1]._objValue) &&
                        expressionEval(queries[2]._strOperator, (Comparable) record.getItem(getColIndex(keys,queries[2]._strColumnName)), (Comparable) queries[2]._objValue)
                )
                    result.add(record);
            }

        }

        return result;
    }

    public static boolean[] setMinMaxQuery(SQLTerm query, Comparable min, Comparable max) throws IOException, ClassNotFoundException, ParseException, DBAppException {

        Comparable[] minMax = new Comparable[2];

        Table table = DBApp.getTable(query._strTableName);

        String colName = query._strColumnName;

        minMax = Table.getMinMaxComparable(colName,table);

        min = minMax[0];
        max = minMax[1];

        boolean includeL = false, includeR = false;

        switch (query._strOperator){
            case "=": {
                includeL = true;
                includeR = true;
                min = (Comparable) query._objValue;
                max = (Comparable) query._objValue;
            };break;
            case ">": {
                includeL = false;
                includeR = true;
                min = (Comparable) query._objValue;
            };break;
            case ">=": {
                includeL = true;
                includeR = true;
                min = (Comparable) query._objValue;
            };break;
            case  "<": {
                includeL = true;
                includeR = false;
                max = (Comparable) query._objValue;
            }break;
            case "<=":{
                includeL = true;
                includeR = true;
                max = (Comparable) query._objValue;
            };break;
        }

        return new boolean[] {includeL, includeR};
    }

}
