package DB;

import com.opencsv.CSVWriter;
import org.apache.commons.collections.bag.SynchronizedSortedBag;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Table implements Serializable {
    private String name;
    private Record prototype;
    private int size;

    private String[] keys;

    public Table(String strTableName,
                 String strClusteringKeyColumn,
                 Hashtable<String, String> htblColNameType,
                 Hashtable<String, String> htblColNameMin,
                 Hashtable<String, String> htblColNameMax)
            throws DBAppException, RuntimeException, IOException {

        this.name = strTableName;
        keys = getKeys(htblColNameType, strClusteringKeyColumn);
        String DBName = DbApp.selectedDBName;

        //Creating a Directory for the table
        new File(DBName + "/" + name).mkdir();

        //Creating a Metadata file for the table
        CSVWriter writer;
        try {
            File metadata = new File(DBName + "/" + name + "/" + "Metadata.csv");
            // create FileWriter object with file as parameter
            FileWriter outputfile = new FileWriter(metadata);
            // create CSVWriter object filewriter object as parameter
            writer = new CSVWriter(outputfile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        writer.writeNext(new String[]{"TableName", "ColumnName", "ColumnType", "ClusteringKey", "IndexName", "IndexType", "min", "max"});

        // add the primary key column
        writer.writeNext(new String[]{strTableName, strClusteringKeyColumn, htblColNameType.get(strClusteringKeyColumn),
                "True", "null", "null", htblColNameMin.get(strClusteringKeyColumn), htblColNameMax.get(strClusteringKeyColumn)});

        // add other columns
        Set<String> allColumns = htblColNameType.keySet();
        for (String columnName : allColumns) {
            //skipping primary key (to not include it twice)
            if (columnName.equals(strClusteringKeyColumn)) {
                continue;
            }
            writer.writeNext(new String[]{strTableName, columnName, htblColNameType.get(columnName),
                    "False", "null", "null", htblColNameMin.get(columnName), htblColNameMax.get(columnName)});
        }
        writer.close();

        // prototype
        try {
            this.prototype = new Record(strClusteringKeyColumn, htblColNameType);

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

    private String[] getMaxAndMinString(String columnName) throws IOException {
        String DBName = DbApp.selectedDBName;
        String csvFile = DBName + "/" + name + "/" + "Metadata.csv";
        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String line = "";
        String cvsSplitBy = ",";
        boolean skipFirstLine = true;
        while ((line = br.readLine()) != null) {
            if (skipFirstLine) {
                skipFirstLine = false;
                continue;
            }
            String[] column = line.split(cvsSplitBy);
            if (column[1].substring(1, column[1].length() - 1).equals(columnName)) {
                return new String[]{column[6].substring(1, column[6].length() - 1), column[7].substring(1, column[7].length() - 1)};
            }
        }

        return null;
    }

    private String[] getKeys(Hashtable<String, String> keysTypes, String clusteringKey) throws IOException {
        String[] res = new String[keysTypes.size()];
        res[0] = clusteringKey;
        int i = 1;
        for (String key : keysTypes.keySet()) {
            if (!key.equals(clusteringKey)) {
                res[i] = key;
                i++;
            }
        }
        return res;
    }

    // checker method to check if the inserted value in the valid range of the key
    private boolean checkValidity(String columnName, Comparable value) throws ParseException, ClassNotFoundException, IOException {
        String[] minAndMax = getMaxAndMinString(columnName);

        String className = value.getClass().getName();

        String minValStr = minAndMax[0], maxValStr = minAndMax[1];
        Comparable minVal, maxVal;
        // get the value of the min and the max
        if (className.equals("java.lang.Integer")) {
            minVal = Integer.parseInt(minValStr);
            maxVal = Integer.parseInt(maxValStr);
        } else if (className.equals("java.lang.Date")) {
            SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD");
            minVal = formatter.parse(minValStr);
            maxVal = formatter.parse(maxValStr);
        } else if (className.equals("java.lang.Double")) {
            minVal = Double.parseDouble(minValStr);
            maxVal = Double.parseDouble(maxValStr);
        } else {
            minVal = minValStr;
            maxVal = maxValStr;
        }

        if (minVal.compareTo(value) > 0) {
            return false;
        }
        if (maxVal.compareTo(value) < 0) {
            return false;
        }
        return true;
    }

    private String getClusteringKey(String tableName) throws IOException {
        String DBName = DbApp.selectedDBName;
        String csvFile = DBName + "/" + tableName + "/" + "Metadata.csv";
        BufferedReader br = new BufferedReader(new FileReader(csvFile));

        br.readLine();
        String line = br.readLine();
        String[] column = line.split(",");
        if (column[3].substring(1, column[3].length() - 1).equals("True")) {
            return column[1].substring(1, column[1].length() - 1);
        }

        return null;
    }

    private boolean checkRecord(Record r){
        for (int i = 0; i < r.getDBVector().size(); i++) {
            try {
                if (!checkValidity(keys[i], (Comparable) r.getDBVector().get(i))) {
                    return false;
                }
            } catch (ParseException | ClassNotFoundException | IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }
    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws
            DBAppException, IOException, ClassNotFoundException, CloneNotSupportedException, ParseException {

        String clusteringKey = getClusteringKey(strTableName);

        // singleton design pattern constraint
        Record record = (Record) prototype.clone();
        record.getDBVector().set(0, htblColNameValue.get(clusteringKey));


        for (int keyIndex = 1; keyIndex < keys.length; keyIndex++) {


            record.getDBVector().set(keyIndex, htblColNameValue.get(keys[keyIndex]));
        }

        checkRecord(record);

        TablePersistence.insert(strTableName, record);
        size++;
    }

    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException, ParseException, CloneNotSupportedException {
        String clusteringKey = getClusteringKey(strTableName);
        // singleton design pattern constraint
        Record record = (Record) prototype.clone();
        record.getDBVector().set(0, htblColNameValue.get(clusteringKey));
        for (int keyIndex = 1; keyIndex < record.getDBVector().size(); keyIndex++) {
            record.getDBVector().set(keyIndex, htblColNameValue.get(keys[keyIndex]));
        }

        TablePersistence.delete(strTableName, record);
        size--;
    }

    public static void setNumberOfPagesForTable(String name, int x) {
        Properties prop = new Properties();
        String fileName = DbApp.currentDBFile + File.separator + "DBApp.config";
        try {
            FileInputStream is = new FileInputStream(fileName);
            prop.load(is);
            prop.setProperty(name + "TablePages", x + "");
            FileOutputStream os = new FileOutputStream(fileName);
            prop.store(os, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Comparable getValue(String val, String type) {
        if (type.equals("java.lang.Integer")) {
            return Integer.parseInt(val);
        } else if (type.equals("java.lang.double")) {
            return Double.parseDouble(val);
        } else if (type.equals("java.lang.Boolean")) {
            return Boolean.parseBoolean(val);
        } else if (type.equals("java.util.Date")) {
            SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD");
            try {
                return formatter.parse(val);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return val;
    }
    public void updateTable(String strTableName, String clusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, CloneNotSupportedException, ClassNotFoundException {
        // singleton design pattern constraint
        Record record = (Record) prototype.clone();
        record.getDBVector().set(0, getValue(clusteringKeyValue, record.getDBVector().get(0).getClass().getName()));


        for (int keyIndex = 1; keyIndex < keys.length; keyIndex++) {

            record.getDBVector().set(keyIndex, htblColNameValue.get(keys[keyIndex]));
        }

        checkRecord(record);
        TablePersistence.update(strTableName, record);
    }

    public static int getNumberOfPagesForTable(String name) {
        Properties prop = new Properties();
        String DBName = DbApp.selectedDBName;
        String fileName = DbApp.currentDBFile + File.separator + "DBApp.config";
        try {
            FileInputStream is = new FileInputStream(fileName);
            prop.load(is);
//            System.out.println(name+" "+prop.getProperty(name+"TablePages"));
            return Integer.parseInt(prop.getProperty(name + "TablePages"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public String toString() {
        try {
            return TablePersistence.printTable(this.name);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws DBAppException {
        try {
            new Table("test", null, null, null, null);
        } catch (DBAppException | IOException e) {
            throw new RuntimeException(e);
        }
    }
}
