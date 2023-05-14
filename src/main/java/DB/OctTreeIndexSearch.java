package DB;

import java.io.IOException;
import java.text.ParseException;

public class OctTreeIndexSearch extends SearchStrategy{
    public static DBVector<Record> Search (SQLTerm[] queries){
        SQLTerm FirstQuery = queries[0];
        SQLTerm SecondQuery = queries[0];
        SQLTerm ThirdQuery = queries[0];

        Comparable minx, maxx, miny, maxy, minz, maxz;


        return null;
    }

    public static Comparable[] setMinMaxQuery(SQLTerm query) throws IOException, ClassNotFoundException, ParseException {

        Comparable[] minmax = new Comparable[2];

        Table table = DBApp.getTable(query._strTableName);


        String colName = query._strColumnName;
        String[] minmaxstring = table.getMaxAndMinString(colName);
        String minmaxclass = table.getKeyType(colName);


        minmax = Table.getMinMaxType(minmaxstring,minmaxclass);



        switch (query._strOperator){
            case "=": {
                minmax[0] = query._objValue;
                minmax[1] = query._objValue;
            };
            case ">": {

            };
            case ">=": {
                minmax[0] = query._objValue;
            };
           // case  "<": return c<0;
            case "<=":{
                minmax[1] = query._objValue;
            };
            //case "!=": return c!=0;
        }

        return null;
    }

}
