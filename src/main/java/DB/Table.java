package DB;

import java.util.Hashtable;

public class Table {
    private String name;
    private Row prototype;
    private int size;
    public Table (String strTableName,
                  String strClusteringKeyColumn,
                  Hashtable<String,String> htblColNameType,
                  Hashtable<String,String> htblColNameMin,
                  Hashtable<String,String> htblColNameMax )
            throws DBAppException{
        this.name=name;
        try {
            this.prototype = new Row(strClusteringKeyColumn,htblColNameType,htblColNameMin,htblColNameMax);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        size=0;
    }
    public int getSize(){
        return this.size;
    }
    public String getName() {
        return name;
    }
}
