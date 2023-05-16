package DB;

import com.opencsv.CSVWriter;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Table implements Serializable {
    private String name;
    private Record prototype;
    private int size;

    private String[] keys;
    private String[] indexCols;
    private Node indexRoot;

    public Table(String strTableName,
            String strClusteringKeyColumn,
            Hashtable<String, String> htblColNameType,
            Hashtable<String, String> htblColNameMin,
            Hashtable<String, String> htblColNameMax)
            throws DBAppException, RuntimeException, IOException {

        this.name = strTableName;
        keys = getKeys(htblColNameType, strClusteringKeyColumn);
        String DBName = DBApp.selectedDBName;

        // Creating a Directory for the table
        new File(DBName + "/" + name).mkdir();

        // Creating a Metadata file for the table
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
        writer.writeNext(new String[] { "TableName", "ColumnName", "ColumnType", "ClusteringKey", "IndexName",
                "IndexType", "min", "max" });

        // add the primary key column
        writer.writeNext(
                new String[] { strTableName, strClusteringKeyColumn, htblColNameType.get(strClusteringKeyColumn),
                        "True", "null", "null", htblColNameMin.get(strClusteringKeyColumn),
                        htblColNameMax.get(strClusteringKeyColumn) });

        // add other columns
        Set<String> allColumns = htblColNameType.keySet();
        for (String columnName : allColumns) {
            // skipping primary key (to not include it twice)
            if (columnName.equals(strClusteringKeyColumn)) {
                continue;
            }
            writer.writeNext(new String[] { strTableName, columnName, htblColNameType.get(columnName),
                    "False", "null", "null", htblColNameMin.get(columnName), htblColNameMax.get(columnName) });
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

    private boolean checkValidType(String type, String val) {
        if (type.toLowerCase().equals("java.lang.integer")) {
            try {
                Integer.parseInt(val);
            } catch (Exception e) {
                return false;
            }
        } else if (type.toLowerCase().equals("java.lang.double")) {
            try {
                Double.parseDouble(val);
            } catch (Exception e) {
                return false;
            }
        } else if (type.toLowerCase().equals("java.util.date")) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-DD");
                sdf.parse(val);
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }

    private boolean checkTypes(String type, String minVal, String maxVal) {
        return checkValidType(type, minVal) && checkValidType(type, maxVal);
    }

    public int getSize() {
        return this.size;
    }

    public String getName() {
        return name;
    }

    public String[] getIndexCols() {
        return this.indexCols;
    }

    public void setIndexCols(String[] indexCols) {
        this.indexCols = indexCols;
    }

    public Node getIndexRoot() {
        return this.indexRoot;
    }

    public void setIndexRoot(Node indexRoot) {
        this.indexRoot = indexRoot;
    }

    public String[] getMaxAndMinString(String columnName) throws IOException {
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
                return new String[] { column[6].substring(1, column[6].length() - 1),
                        column[7].substring(1, column[7].length() - 1) };
            }
        }

        return null;
    }

    public static Comparable[] getMinMaxType(String[] colMinMax, String className) throws ParseException {
        Comparable min = colMinMax[0], max = colMinMax[1];

        if (className.toLowerCase().equals("java.lang.integer")) {
            min = Integer.parseInt(colMinMax[0]);
            max = Integer.parseInt(colMinMax[1]);
        } else if (className.toLowerCase().equals("java.lang.double")) {
            min = Double.parseDouble(colMinMax[0]);
            max = Double.parseDouble(colMinMax[1]);
        } else if (className.toLowerCase().equals("java.lang.date")) {
            SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD");
            min = formatter.parse(colMinMax[0]);
            max = formatter.parse(colMinMax[1]);
        }

        return new Comparable[] { min, max };
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

    public String getKeyType(String key) {
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

    public static Comparable[] getMinMaxComparable(String columnName, Table table)
            throws IOException, ParseException, DBAppException {
        String[] minAndMax = table.getMaxAndMinString(columnName);

        String className = table.getKeyType(columnName);

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
        } catch (Exception e) {
            throw new DBAppException("Invalid Value for column " + columnName);
        }
        return new Comparable[] { minVal, maxVal };
    }

    private boolean checkValidity(String columnName, Comparable value)
            throws ParseException, ClassNotFoundException, IOException, DBAppException {
        // get the value of the min and the max
        try {
            Comparable[] minMax = getMinMaxComparable(columnName, this);
            Comparable minVal = minMax[0], maxVal = minMax[1];
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

    private boolean checkRecord(Record r) throws DBAppException {
        for (int i = 0; i < r.getDBVector().size(); i++) {
            try {
                if (r.getItem(i) != null && !checkValidity(keys[i], (Comparable) r.getDBVector().get(i))) {
                    return false;
                }
            } catch (ParseException | ClassNotFoundException | IOException e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    private Record getRecord(Hashtable<String, Object> htblColNameValue)
            throws DBAppException, CloneNotSupportedException {
        Record record = (Record) prototype.clone();
        for (int keyIndex = 0; keyIndex < keys.length; keyIndex++) {
            if (!htblColNameValue.containsKey(keys[keyIndex])) {
                record.setItem(keyIndex, null);
            } else
                record.getDBVector().set(keyIndex, htblColNameValue.get(keys[keyIndex]));
        }
        return record;
    }

    public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException, CloneNotSupportedException, ParseException {

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

    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, ClassNotFoundException, ParseException, CloneNotSupportedException {

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

    private Comparable getValue(String val, String type) throws DBAppException {
        if (!checkValidType(type, val))
            throw new DBAppException("Invalid value for type " + type + " : " + val);

        if (type.toLowerCase().equals("java.lang.integer")) {
            return Integer.parseInt(val);
        } else if (type.toLowerCase().equals("java.lang.double")) {
            return Double.parseDouble(val);
        } else if (type.toLowerCase().equals("java.util.date")) {
            SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD");
            try {
                return formatter.parse(val);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return val;
    }

    public void updateTable(String strTableName, String clusteringKeyValue, Hashtable<String, Object> htblColNameValue)
            throws DBAppException, IOException, CloneNotSupportedException, ClassNotFoundException {

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
            // System.out.println(name+" "+prop.getProperty(name+"TablePages"));
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


    public int[] getAllMaxValuesString() throws IOException {
        String DBName = DBApp.selectedDBName;
        String csvFile = DBName + "/" + name + "/" + "metadata.csv";
        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String line;
        String cvsSplitBy = ",";
        ArrayList<Integer> integerArrayList = new ArrayList<>();
        boolean skipFirstLine = true;
        while ((line = br.readLine()) != null) {
            if (skipFirstLine) {
                skipFirstLine = false;
                continue;
            }
            String[] column = line.split(cvsSplitBy);
            integerArrayList.add(column[7].length());
        }
        int[] max = new int[integerArrayList.size()];
        for (int i = 0; i < integerArrayList.size(); i++) {
            max[i] = integerArrayList.get(i);
        }
        return max;
    }

    public static Node createRootNode(Table table, String[] strarrColName)
            throws IOException, ParseException, DBAppException {

        Comparable[] minMaxX = getMinMaxComparable(strarrColName[0], table);
        Comparable[] minMaxY = getMinMaxComparable(strarrColName[1], table);
        Comparable[] minMaxZ = getMinMaxComparable(strarrColName[2], table);

        return new Node(minMaxX[0], minMaxY[0], minMaxZ[0], minMaxX[1], minMaxY[1], minMaxZ[1]);
    }

    public static Point3D createPoint(Table table, Record r, String[] strarrColName) {
        int xIndex = -1, yIndex = -1, zIndex = -1;

        for (int i = 0; i < table.keys.length; i++) {
            if (table.keys[i].equals(strarrColName[0])) {
                xIndex = i;
            }
            if (table.keys[i].equals(strarrColName[1])) {
                yIndex = i;
            }
            if (table.keys[i].equals(strarrColName[2])) {
                zIndex = i;
            }
        }

        Comparable x = (Comparable) r.getDBVector().get(xIndex);
        Comparable y = (Comparable) r.getDBVector().get(yIndex);
        Comparable z = (Comparable) r.getDBVector().get(zIndex);

        return new Point3D(x, y, z);

    }

    public static void insertLinearIntoIndex(String strTableName, Node root, String[] strarrColName)
            throws IOException, ClassNotFoundException, DBAppException, ParseException {
        Table table = DBApp.getTable(strTableName);

        if (table == null) {
            throw new DBAppException("Table not found!");
        }

        root = createRootNode(table, strarrColName);

        int n = Table.getNumberOfPagesForTable(strTableName);
        for (int i = 0; i < n; i++) {
            Page p = TablePersistence.deserialize(i, strTableName);
            DBVector<Record> records = p.getRecords();
            for (Record r : records) {
                Point3D point = createPoint(table, r, strarrColName);
                root.insert(point, i);
            }
            TablePersistence.serialize(p, strTableName, i);
        }

        table.setIndexCols(strarrColName);
        table.setIndexRoot(root);
    }

    public static void createIndex(String strTableName,
            String[] strarrColName) throws DBAppException, IOException, ClassNotFoundException, ParseException {

        insertLinearIntoIndex(strTableName, null, strarrColName);

    }


    public DBVector<Record> selectFromTable(String strTableName, SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException, IOException, ParseException, ClassNotFoundException, CloneNotSupportedException {
        rearrangeQueries3(arrSQLTerms, strarrOperators);
        return selectHelper(strTableName, arrSQLTerms, strarrOperators, 0);

    }

    public DBVector<Record> selectHelper(String strTableName, SQLTerm[] arrSQLTerms, String[] strarrOperators, int x) throws IOException, ClassNotFoundException, CloneNotSupportedException, DBAppException, ParseException {
        if(x == strarrOperators.length)
            return null;
        if(useOctree(new SQLTerm[]{arrSQLTerms[x], arrSQLTerms[x+1], arrSQLTerms[x+2]}, new String[] {strarrOperators[x], strarrOperators[x+1]})){
            return handleOperators(OctTreeIndexSearch.Search(new SQLTerm[]{arrSQLTerms[x], arrSQLTerms[x+1], arrSQLTerms[x+2]} ,this.keys),
                    selectHelper(strTableName,arrSQLTerms,strarrOperators,x+3)
                    ,strarrOperators[x]);
        }
        else if(arrSQLTerms[x]._strColumnName.equals(keys[0])){
            return handleOperators(ClusteringKeySearch.Search(arrSQLTerms[0],this.keys, this.prototype),
                    selectHelper(strTableName,arrSQLTerms,strarrOperators,x+1)
                    ,strarrOperators[x]);
        }
        else {
            return handleOperators(LinearSearch.Search(arrSQLTerms[0],this.keys),
                    selectHelper(strTableName,arrSQLTerms,strarrOperators,x+1)
                    ,strarrOperators[x]);
        }
    }

    public boolean useOctree(SQLTerm[] queries, String[] operators){
        String[] indexCols = this.indexCols;

        HashSet<String> hs = new HashSet<>();
        hs.add(indexCols[0]);
        hs.add(indexCols[1]);
        hs.add(indexCols[2]);

        if(operators[0].equals("AND") && operators[1].equals("AND")){
            hs.remove(queries[0]._strColumnName);
            hs.remove(queries[1]._strColumnName);
            hs.remove(queries[2]._strColumnName);
        }

        return hs.size() == 0;
    }

    public DBVector<Record> handleOperators(DBVector<Record>FirstSet,DBVector<Record>SecondSet,String operator){
        if(SecondSet == null) return FirstSet;
        switch (operator){
            case "AND": return Table.and(FirstSet,SecondSet);
            case "OR": return  Table.or(FirstSet,SecondSet);
            case "XOR": return  Table.xor(FirstSet,SecondSet);
            default: return null;
        }
    }

    public static DBVector<Record> and(DBVector<Record>FirstSet, DBVector<Record>SecondSet){
        HashSet<Record> FirstSetHashTable = new HashSet<Record>();
        DBVector<Record> result = new DBVector<Record>();

        for(Record record:FirstSet)FirstSetHashTable.add(record);
        for(Record record:SecondSet)if(FirstSetHashTable.contains(record))result.add(record);
        return result;
    }

    public static DBVector<Record> or(DBVector<Record> FirstSet, DBVector<Record>SecondSet){
        HashSet<Record> resultHashSet = new HashSet<Record>();
        DBVector<Record>  result = new DBVector<Record>();

        for(Record record:FirstSet) resultHashSet.add(record);
        for(Record record:SecondSet) resultHashSet.add(record);
        for(Record record:resultHashSet) result.add(record);

        return result;
    }

    public static DBVector<Record> xor(DBVector<Record> FirstSet, DBVector<Record> SecondSet){
        HashSet<Record> resultHashSet = new HashSet<Record>();
        DBVector<Record>  result = new DBVector<Record>();

        for(Record record:FirstSet) resultHashSet.add(record);
        for(Record record:SecondSet) {
            if(resultHashSet.contains(record))resultHashSet.remove(record);
            else resultHashSet.add(record);
        }
        for(Record record:resultHashSet) result.add(record);

        return result;
    }




    public void rearrangeQueries3(SQLTerm[] sqlTermArr, String[] operators) {
        ArrayList<int[]> indices = new ArrayList<>();

        int start = 0;
        for (int i = 0; i < operators.length; i++) {
            if (!operators[i].equals("AND")) {
                int[] a = new int[]{start, i};
                indices.add(a);
                start = i + 1;
            }

            if (operators[i].equals("AND") && i == operators.length - 1) {
                int[] a = new int[2];
                a = new int[]{start, i + 1};
                indices.add(a);
                start = i + 1;
            }

            for (int[] arr : indices) {
                this.rearrangeQueries3helper(sqlTermArr, arr[0], arr[1]);
            }

        }
    }

    public void rearrangeQueries3helper(SQLTerm[] sqlTermArr, int start, int end){
        if(end-start<=2)return;
        String[] indexCols = this.indexCols;

        LinkedList<SQLTerm> dimXTerms = new LinkedList<SQLTerm>();
        LinkedList<SQLTerm> dimYTerms = new LinkedList<SQLTerm>();
        LinkedList<SQLTerm> dimZTerms = new LinkedList<SQLTerm>();
        LinkedList<SQLTerm> otherTerms= new LinkedList<SQLTerm>();
        for(int i=start;i<=end;i++){
            if(sqlTermArr[i]._strColumnName.equals(indexCols[0]))dimXTerms.add(sqlTermArr[i]);
            else if(sqlTermArr[i]._strColumnName.equals(indexCols[1]))dimYTerms.add(sqlTermArr[i]);
            else if(sqlTermArr[i]._strColumnName.equals(indexCols[2]))dimZTerms.add(sqlTermArr[i]);
            else otherTerms.add(sqlTermArr[i]);
        }



        while(dimXTerms.size()>0 || dimYTerms.size()>0 || dimZTerms.size()>0 || otherTerms.size()>0){
            if(dimXTerms.size()>0){
                sqlTermArr[start] = dimXTerms.removeFirst();
                start++;
            }
            if(dimYTerms.size()>0){
                sqlTermArr[start] = dimYTerms.removeFirst();
                start++;
            }
            if(dimZTerms.size()>0){
                sqlTermArr[start] = dimZTerms.removeFirst();
                start++;
            }
            if(otherTerms.size()>0){
                sqlTermArr[start] = otherTerms.removeFirst();
                start++;
            }
        }


    }
    //  0            2         4     5
    //  and   and  and   or   and  and
    // i     i    i    i   i      i    i
    // 0               3   4            6


    // x and x and x and y and y and y and z and z and z

    public static void main(String[] args) {
        SQLTerm query1 = new SQLTerm();
        query1._strColumnName = "col1";
        SQLTerm query2 = new SQLTerm();
        query2._strColumnName = "col4";
        SQLTerm query3 = new SQLTerm();
        query3._strColumnName = "col4";
        SQLTerm query4 = new SQLTerm();
        query4._strColumnName = "col2";
        SQLTerm query5 = new SQLTerm();
        query5._strColumnName = "col2";
        SQLTerm query6 = new SQLTerm();
        query6._strColumnName = "col5";
        SQLTerm query7 = new SQLTerm();
        query7._strColumnName = "col3";
        SQLTerm query8 = new SQLTerm();
        query8._strColumnName = "col3";
        SQLTerm query9 = new SQLTerm();
        query9._strColumnName = "col1";


        String[] operators = new String[] {"AND", "AND", "AND", "AND", "AND", "AND", "AND", "AND"};

        SQLTerm[] queries = new SQLTerm[] {query1,query2, query3, query4, query5, query6, query7, query8, query9};


    }
}
