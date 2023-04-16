package DB;

import com.opencsv.CSVWriter;
import org.apache.commons.collections.bag.SynchronizedSortedBag;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
public class Table {
    private String name;
    private Record prototype;
    private int size;

    public Table(String strTableName,
                 String strClusteringKeyColumn,
                 Hashtable<String, String> htblColNameType,
                 Hashtable<String, String> htblColNameMin,
                 Hashtable<String, String> htblColNameMax)
            throws DBAppException, RuntimeException {
        this.name = strTableName;

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

        writer.writeNext(new String[]{"TableName","ColumnName", "ColumnType", "ClusteringKey", "IndexName", "IndexType", "min", "max"});

        // add the primary key column
        writer.writeNext(new String[] {strTableName, strClusteringKeyColumn, htblColNameType.get(strClusteringKeyColumn),
        "True", "null", "null", htblColNameMin.get(strClusteringKeyColumn), htblColNameMax.get(strClusteringKeyColumn)});

        // add other columns

        Set<String> allColumns = htblColNameType.keySet();
        for(String columnName: allColumns){
            if(columnName.equals(strClusteringKeyColumn)){
                continue;
            }
            writer.writeNext(new String[] {strTableName, columnName, htblColNameType.get(columnName),
                    "False", "null", "null", htblColNameMin.get(columnName), htblColNameMax.get(columnName)});
        }

        try {
            this.prototype = new Record(strClusteringKeyColumn,htblColNameType);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        size = 0;
        setNumberOfPagesForTable(this.name,0);
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

        while ((line = br.readLine()) != null) {
            String[] column = line.split(cvsSplitBy);
            if(column[1].equals(columnName)){
                return new String [] {column[6], column[7]};
            }
        }

        return null;
    }
    // checker method to check if the inserted value in the valid range of the key
    private boolean checkValidity(String columnName, Comparable value) throws ParseException, ClassNotFoundException, IOException {
        String[] minAndMax = getMaxAndMinString(columnName);

        String className = value.getClass().getName();

        String minValStr = minAndMax[0], maxValStr = minAndMax[1];
        Comparable minVal, maxVal;
        // get the value of the min and the max
        if(className.equals("java.lang.Integer")){
            minVal = Integer.parseInt(minValStr);
            maxVal = Integer.parseInt(maxValStr);
        }
        else if(className.equals("java.lang.Date")){
            SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD");
            minVal = formatter.parse(minValStr);
            maxVal = formatter.parse(maxValStr);
        }
        else if(className.equals("java.lang.Double")){
            minVal = Double.parseDouble(minValStr);
            maxVal = Double.parseDouble(maxValStr);
        }
        else{
            minVal = minValStr;
            maxVal = maxValStr;
        }

        if(minVal.compareTo(value) > 0){
            return false;
        }
        if(maxVal.compareTo(value) < 0){
            return false;
        }
        return true;
    }
    public static void setNumberOfPagesForTable(String name, int x) {
        Properties prop = new Properties();
        String fileName = "src/main/java/DB/config/DBApp.config";
        try {
            FileInputStream is = new FileInputStream(fileName);
            prop.load(is);
            prop.setProperty("NumberOfPagesOfTable"+name, x+"");
            FileOutputStream os = new FileOutputStream(fileName);
            prop.store(os,null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static int getNumberOfPagesForTable(String name) {
        Properties prop = new Properties();
        String fileName = "src/main/java/DB/config/DBApp.config";
        try {
            FileInputStream is = new FileInputStream(fileName);
            prop.load(is);
            return  Integer.parseInt(prop.getProperty("NumberOfPagesOfTable"+name));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
    public static void main(String[] args) throws DBAppException {
        try {
            new Table("test", null, null, null, null);
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }
    }

}
