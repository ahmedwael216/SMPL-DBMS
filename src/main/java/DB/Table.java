package DB;

import java.io.File;
import java.io.IOException;
import java.util.Hashtable;

public class Table {
    private String name;
    private Row prototype;
    private int size;

    public Table(String strTableName,
                 String strClusteringKeyColumn,
                 Hashtable<String, String> htblColNameType,
                 Hashtable<String, String> htblColNameMin,
                 Hashtable<String, String> htblColNameMax)
            throws DBAppException, RuntimeException {
        this.name = strTableName;
        //TODO load DBName from DBApp.config
        String DBName = "DB1";

        //Creating a Directory for the table
        new File(DBName + "/" + name).mkdir();
        //Creating a Metadata file for the table
        try {
            new File(DBName + "/" + name + "/" + "Metadata.csv").createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        //TODO add column data to csv file

        try {
            //this.prototype = new Row(strClusteringKeyColumn,htblColNameType);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        size = 0;
    }

    public int getSize() {
        return this.size;
    }

    public String getName() {
        return name;
    }

    public static void main(String[] args) throws DBAppException {
        try {
            new Table("test", null, null, null, null);
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }
    }
}
