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
        String DBName = DBApp.selectedDBName;

        //Creating a Directory for the table
        new File(DBName + "/" + name).mkdir();

        //Creating a Metadata file for the table
        CSVWriter writer;
        try {
            File metadata = new File(DBName + "/" + name + "/" + "metadata.csv");
            // create FileWriter object with file as parameter
            FileWriter outputfile = new FileWriter(metadata);
            // create CSVWriter object filewriter object as parameter
            writer = new CSVWriter(outputfile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        for (Map.Entry<String, String> entry : htblColNameType.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (!checkTypes(value, htblColNameMin.get(key), htblColNameMax.get(key)))
                throw new DBAppException("Invalid Value for column " + key);
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

    private boolean checkTypes(String type, String minVal, String maxVal) {
        if (type.toLowerCase().equals("java.lang.integer")) {
            try {
                Integer.parseInt(minVal);
                Integer.parseInt(maxVal);
            } catch (Exception e) {
                return false;
            }
        } else if (type.toLowerCase().equals("java.lang.double")) {
            try {
                Double.parseDouble(minVal);
                Double.parseDouble(maxVal);
            } catch (Exception e) {
                return false;
            }
        } else if (type.toLowerCase().equals("java.util.date")) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                sdf.parse(minVal);
                sdf.parse(maxVal);
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    public int getSize() {
        return this.size;
    }

    public String getName() {
        return name;
    }

    private String[] getMaxAndMinString(String columnName) throws IOException {
        String DBName = DBApp.selectedDBName;
        String csvFile = DBName + "/" + name + "/" + "metadata.csv";
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

    private String getKeyType(String key) {
        String DBName = DBApp.selectedDBName;
        String csvFile = DBName + "/" + name + "/" + "metadata.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        boolean skipFirstLine = true;
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                if (skipFirstLine) {
                    skipFirstLine = false;
                    continue;
                }
                String[] column = line.split(cvsSplitBy);
                if (column[1].substring(1, column[1].length() - 1).equals(key)) {
                    return column[2].substring(1, column[2].length() - 1);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private boolean checkValidity(String columnName, Comparable value) throws ParseException, ClassNotFoundException, IOException, DBAppException {
        String[] minAndMax = getMaxAndMinString(columnName);

        String className = getKeyType(columnName);

        String minValStr = minAndMax[0], maxValStr = minAndMax[1];
        Comparable minVal, maxVal;
        // get the value of the min and the max
        try {
            if (className.toLowerCase().equals("java.lang.integer")) {
                minVal = Integer.parseInt(minValStr);
                maxVal = Integer.parseInt(maxValStr);
            } else if (className.toLowerCase().equals("java.util.date")) {
                SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD");
                minVal = formatter.parse(minValStr);
                maxVal = formatter.parse(maxValStr);
            } else if (className.toLowerCase().equals("java.lang.double")) {
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
        } catch (Exception e) {
            throw new DBAppException("Invalid value for column " + columnName);
        }
        return true;
    }

    private boolean checkRecord(Record r) throws DBAppException{
        for (int i = 0; i < r.getDBVector().size(); i++) {
            try {
                if (r.getItem(i) != null  && !checkValidity(keys[i], (Comparable) r.getDBVector().get(i))) {
                    return false;
                }
            } catch (ParseException | ClassNotFoundException | IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    private Record getRecord(Hashtable<String, Object> htblColNameValue) throws DBAppException, CloneNotSupportedException {
        Record record = (Record) prototype.clone();
        for (int keyIndex = 0; keyIndex < keys.length; keyIndex++) {
            if (!htblColNameValue.containsKey(keys[keyIndex])) {
                record.setItem(keyIndex, null);
            } else
                record.getDBVector().set(keyIndex, htblColNameValue.get(keys[keyIndex]));
        }
        return record;
    }

    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws
            DBAppException, IOException, ClassNotFoundException, CloneNotSupportedException, ParseException {

        String clusteringKey = keys[0];
        if (!htblColNameValue.containsKey(clusteringKey)) {
            throw new DBAppException("the clustering key must be inserterd");
        }

        Record record = getRecord(htblColNameValue);

        if (!checkRecord(record)) {
            throw new DBAppException("Please enter valid data");
        }

        TablePersistence.insert(strTableName, record);
        size++;
    }

    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, ClassNotFoundException, ParseException, CloneNotSupportedException {

        Record record = getRecord(htblColNameValue);

        size -= TablePersistence.delete(strTableName, record);
    }

    public static void setNumberOfPagesForTable(String name, int x) {
        Properties prop = new Properties();
        String fileName = DBApp.currentDBFile + File.separator + "DBApp.config";
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
        if (type.toLowerCase().equals("java.lang.integer")) {
            return Integer.parseInt(val);
        } else if (type.toLowerCase().equals("java.lang.double")) {
            return Double.parseDouble(val);
        } else if (type.toLowerCase().equals("java.util.date")) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            try {
                return formatter.parse(val);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return val;
    }

    public void updateTable(String strTableName, String clusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException, CloneNotSupportedException, ClassNotFoundException {

        Record record = (Record) prototype.clone();
        record.getDBVector().set(0, getValue(clusteringKeyValue, record.getDBVector().get(0).getClass().getName()));


        for (int keyIndex = 1; keyIndex < keys.length; keyIndex++) {

            record.getDBVector().set(keyIndex, htblColNameValue.get(keys[keyIndex]));
        }

        if (!checkRecord(record)) {
            throw new DBAppException("Please enter valid data");
        }

        TablePersistence.update(strTableName, record);

    }

    public static int getNumberOfPagesForTable(String name) {
        Properties prop = new Properties();
        String DBName = DBApp.selectedDBName;
        String fileName = DBApp.currentDBFile + File.separator + "DBApp.config";
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
            return TablePersistence.printTable(this.name) + "\n size = " + size;
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
