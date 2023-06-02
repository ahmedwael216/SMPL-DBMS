package DB;

import grammar.SQLiteLexer;
import grammar.SQLiteParser;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.*;
import java.text.ParseException;
import java.util.*;
import org.junit.jupiter.api.Assertions;

import javax.swing.border.TitledBorder;
//import java.util.Iterator;


public class DBApp {
    public static int maxRecordsCountPage;
    // TODO: get the value of maxEntriesCountNode and change it
    public static int maxEntriesCountNode = 4;
    // Path to SMPL-DBMS
    public static String rootPath = new File(System.getProperty("user.dir")).getAbsolutePath();//.getParentFile().getParentFile().getParentFile().getParent() + File.separator;
    public static String selectedDBName = null;

    public static String DATE_FORMAT = "yyyy-MM-dd";
    public static File currentDBFile = null;
    public static File currentConfigFile = null;

    /**
     * Executes at application startup.
     * Prompts user to enter name of database to use.
     *
     * @throws IOException if an error occurred while inputting or outputting
     */
    public void init() {
//        // read available databases doesn't work
//        // Store currently available database names
//        DBVector<String> availableDatabases = new DBVector<>();
//        DBVector<String> excludedDirectories = new DBVector<>();
//        Collections.addAll(excludedDirectories, ".metadata", ".git", "src", "target");
//        File rootFile = new File(rootPath);
//        String[] rootContents = rootFile.list();
//        for (String fileName : rootContents) {
//            File file = new File(rootPath + File.separator + fileName);
//            if (file.isDirectory() && !excludedDirectories.contains(fileName)) {
//                availableDatabases.add(fileName.toLowerCase()); // toLowerCase to avoid logic errors because Windows file system is case insensitive
//            }
//        }
//
//        // Prompt user for database name
//        System.out.println("\nHello " + System.getProperty("user.name") + ", welcome to our Simple Database Management System!");
//        System.out.println("Please enter the database name you would like to load.");
//        System.out.println("Allowed characters: A-Z, 0-9, underscore (case in-sensitive)\n\n");
//        Scanner sc = new Scanner(System.in);
//        do {
//            System.out.print("Load database: ");
//            selectedDBName = sc.nextLine();
//        } while (selectedDBName == null || !selectedDBName.matches("^[a-zA-Z0-9_]+")); // ensure proper naming of input database name
//        sc.close();
//
//        // Moving to selected database
//        if (availableDatabases.contains(selectedDBName.toLowerCase())) {
//            System.out.println("Switching context to exisitng database: \"" + selectedDBName + "\"");
//        } else {
//            System.out.println("Creating new database: \"" + selectedDBName + "\"");
//            File newDatabase = new File(rootPath + File.separator + selectedDBName);
//            if (newDatabase.mkdir()) {
//                DBVector<String> lines = new DBVector<>();
//                lines.add("databaseName = " + selectedDBName);
//                lines.add("maximumNumberOfRows = 200");
//                File newConfig = new File(rootPath + File.separator + selectedDBName + File.separator + "DBApp.config");
//                try {
//                    FileWriter fw = new FileWriter(newConfig);
//                    for (String line : lines) {
//                        fw.append(line + "\n");
//                    }
//                    fw.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            } else {
//                System.err.println("Could not create the directory \"" + newDatabase.getAbsolutePath() + "\"");
//            }
//        }
//        currentDBFile = new File(rootPath + File.separator + selectedDBName);
//        currentConfigFile = new File(currentDBFile.getAbsolutePath() + File.separator + "DBApp.config");
    }

    public DBApp() {
        selectedDBName = "src/main/resources";
        currentDBFile = new File(rootPath + File.separator + selectedDBName);
//        System.out.println(currentDBFile.getAbsolutePath());

        currentConfigFile = new File(currentDBFile.getAbsolutePath() + File.separator + "DBApp.config");
//        System.out.println(currentConfigFile.getAbsolutePath());

        if (currentConfigFile.exists()) {
            System.out.println("Switching context to existing database: ");
            getMaximumRecordsCountPage();
            getmaxEntriesCountNode();
        } else {
            System.out.println("creating database in resources folder");

            DBVector<String> lines = new DBVector<>();
            lines.add("databaseName = " + selectedDBName);
            lines.add("maximumNumberOfRows = 200");
            lines.add("maxEntriesCountNode = 4");
            maxRecordsCountPage = 200;
            try {
                FileWriter fw = new FileWriter(currentConfigFile);
                for (String line : lines) {
                    fw.append(line).append("\n");
                }
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /*
     * Appends to current database config file the table's name with initially 0 pages.
     * Does nothing if table already exists
     *
     * @param strTableName              name of the table to create
     * @param strClusteringKeyColumn    name of the column to be the primary key and the clustering column of this table
     * @param htblColNameType           maps names of columns to their types
     * @param htblColNameMin            maps names of columns to minimum accepted value
     * @param htblColNameMax            maps names of columns to maximum accepted value
     *
     * @throws DBAppException           if an exception occurred
     */
    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType,
                            Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax)
            throws DBAppException {
        Properties prop = new Properties();
        try {
            strTableName = strTableName.toLowerCase();
            prop.load(new FileInputStream(currentConfigFile.getAbsolutePath()));
            if (prop.getProperty(strTableName + "TablePages") != null) {
                System.err.println("The table \"" + strTableName + "\" already exists in the database \"" + selectedDBName + "\"");
            } else {
                prop.setProperty(strTableName + "TablePages", "0"); // Initialize the new table with 0 pages in config of current DB
                prop.store(new FileWriter(currentConfigFile.getAbsolutePath()), "Created " + strTableName + " table");
                Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax);
                FileOutputStream file = new FileOutputStream(currentConfigFile.getParent() + File.separator + strTableName + File.separator + strTableName + ".ser");
                ObjectOutputStream out = new ObjectOutputStream(file);
                out.writeObject(t);
                out.close();
                file.close();

                System.out.println("The table \"" + strTableName + "\" has been added to the database \"" + selectedDBName + "\"");
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getMaximumRecordsCountPage() {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(currentConfigFile.getAbsolutePath()));
            maxRecordsCountPage = Integer.parseInt(prop.getProperty("maximumNumberOfRows"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getmaxEntriesCountNode() {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(currentConfigFile.getAbsolutePath()));
            maxEntriesCountNode = Integer.parseInt(prop.getProperty("maxEntriesCountNode"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // following method creates an octree
    // depending on the count of column names passed.
    // If three column names are passed, create an octree.
    // If only one or two column names is passed, throw an Exception.
    public void createIndex(String strTableName,
                            String[] strarrColName)
            throws DBAppException, IOException, ClassNotFoundException, ParseException {
        if (strarrColName.length != 3)
            throw new DBAppException("There must be three columns in order to create the index.");


        Properties prop = new Properties();
        try {
            strTableName = strTableName.toLowerCase();
            prop.load(new FileInputStream(currentConfigFile.getAbsolutePath()));
            if (prop.getProperty(strTableName + "TablePages") != null) {
                prop.store(new FileWriter(currentConfigFile.getAbsolutePath()), "create index for " + strTableName + " table");

                Table table = getTable(strTableName);
                System.out.println("Here " + strTableName);
                table.createIndex(strTableName, strarrColName);
                System.out.println("Here2 " + table.getTableIndices().size());
                FileOutputStream fileOut = new FileOutputStream(currentConfigFile.getParent() + File.separator + strTableName + File.separator + strTableName + ".ser");
                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                out.writeObject(table);
                out.close();
                System.out.println("Here3 " + table.getTableIndices().size() + " " + table.test.size());
            } else {
                System.err.println("The table \"" + strTableName + "\" does not exist in the database \"" + selectedDBName + "\"");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    /**
     * Inserts a new row in a specified table.
     * Inserted data must have a value for the primary key.
     *
     * @param strTableName     name of the table to insert the row into
     * //@param htblColNameValue maps columns' names to their corresponding values to be inserted
     * @throws DBAppException If an exception occurred
     */
    public void serilizeTable(String strTableName, Table table) throws IOException, ClassNotFoundException {
        FileOutputStream fileOut = new FileOutputStream(currentConfigFile.getParent() + File.separator + strTableName + File.separator + strTableName + ".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(table);
        out.close();
    }
    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        Properties prop = new Properties();
        try {
            strTableName = strTableName.toLowerCase();
            prop.load(new FileInputStream(currentConfigFile.getAbsolutePath()));
            if (prop.getProperty(strTableName + "TablePages") != null) {
                prop.store(new FileWriter(currentConfigFile.getAbsolutePath()), "Insert into " + strTableName + " table");

                Table table = getTable(strTableName);

                table.insertIntoTable(strTableName, htblColNameValue);

                serilizeTable(strTableName, table);

            } else {
                System.err.println("The table \"" + strTableName + "\" does not exist in the database \"" + selectedDBName + "\"");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Updates a row in a specified table.
     *
     * @param strTableName          name of the table to update the row from
     * @param strClusteringKeyValue the value to look for to find the row to update
     * @param htblColNameValue      maps the column names and their corresponding new values to be updated.
     *                              Cannot include clustering key as column name.
     * @throws DBAppException If an exception occurred
     */
    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        if (strClusteringKeyValue.equals("null")) {
            throw new DBAppException("the clustering key must be inserterd");
        }
        Properties prop = new Properties();
        try {
            strTableName = strTableName.toLowerCase();
            prop.load(new FileInputStream(currentConfigFile.getAbsolutePath()));
            if (prop.getProperty(strTableName + "TablePages") != null) {
                prop.store(new FileWriter(currentConfigFile.getAbsolutePath()), "Update " + strTableName + " table");

                Table table = getTable(strTableName);

                table.updateTable(strTableName, strClusteringKeyValue, htblColNameValue);

                serilizeTable(strTableName, table);
            } else {
                System.err.println("The table \"" + strTableName + "\" does not exist in the database \"" + selectedDBName + "\"");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
//        catch (CloneNotSupportedException e) {
//            e.printStackTrace();
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
    }

    /**
     * Delete one or more rows from a specified table.
     * Searches for the values in the specified columns and deletes matching row(s).
     * Entries are anded together.
     *
     * @param strTableName     name of the table to delete row(s) from
     * @param htblColNameValue maps column names to certain value and delete the row(s) if matching(s) found
     * @throws DBAppException If an exception occurred
     */
    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        Properties prop = new Properties();
        try {
            strTableName = strTableName.toLowerCase();
            prop.load(new FileInputStream(currentConfigFile.getAbsolutePath()));
            if (prop.getProperty(strTableName + "TablePages") != null) {
                prop.store(new FileWriter(currentConfigFile.getAbsolutePath()), "Delete From " + strTableName + " table");
                System.out.println(currentConfigFile.getParent() + File.separator + strTableName + File.separator + strTableName + ".ser");

                Table table = getTable(strTableName);

                table.deleteFromTable(strTableName, htblColNameValue);

                serilizeTable(strTableName, table);
            } else {
                System.err.println("The table \"" + strTableName + "\" does not exist in the database \"" + selectedDBName + "\"");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
    }

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators) throws DBAppException {
        String strTableName = arrSQLTerms[0]._strTableName.toLowerCase();

        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(currentConfigFile.getAbsolutePath()));
            if (prop.getProperty(strTableName + "TablePages") != null) {
                prop.store(new FileWriter(currentConfigFile.getAbsolutePath()), "Select From " + strTableName + " table");
                System.out.println(currentConfigFile.getParent() + File.separator + strTableName + File.separator + strTableName + ".ser");

                Table table = getTable(strTableName);

                return table.selectFromTable(strTableName, arrSQLTerms, strarrOperators).iterator();
            } else {
                System.err.println("The table \"" + strTableName + "\" does not exist in the database \"" + selectedDBName + "\"");
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }

        return null;

    }

    public String printTable(String strTableName) throws IOException, DBAppException {
        strTableName = strTableName.toLowerCase();
        Table table = getTable(strTableName);
        String ret = table.toString();

        FileOutputStream fileOut = new FileOutputStream(currentConfigFile.getParent() + File.separator + strTableName + File.separator + strTableName + ".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(table);
        out.close();

        return ret;
    }


    public Iterator parseSQL(StringBuffer strbufSQL) throws DBAppException {
        return SQLParser.parse(strbufSQL, this);
    }


    /**
     * Helper function to check if the column has an index or not.
     *
     * @return index if found or null if not
     */
    //public ??? findIndex(String strTableName,
    //                     String strColName)
    //                     throws DBAppException {}

    private static void  insertCoursesRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader coursesTable = new BufferedReader(new FileReader("src/main/resources/courses_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = coursesTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");


            int year = Integer.parseInt(fields[0].trim().substring(0, 4));
            int month = Integer.parseInt(fields[0].trim().substring(5, 7));
            int day = Integer.parseInt(fields[0].trim().substring(8));

            Date dateAdded = new Date(year - 1900, month - 1, day);

            row.put("date_added", dateAdded);

            row.put("course_id", fields[1]);
            row.put("course_name", fields[2]);
            row.put("hours", Integer.parseInt(fields[3]));

            dbApp.insertIntoTable("courses", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        coursesTable.close();
    }

    private static void  insertStudentRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader studentsTable = new BufferedReader(new FileReader("src/main/resources/students_table.csv"));
        String record;
        int c = limit;
        if (limit == -1) {
            c = 1;
        }

        Hashtable<String, Object> row = new Hashtable<>();
        while ((record = studentsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("id", fields[0]);
            row.put("first_name", fields[1]);
            row.put("last_name", fields[2]);

            int year = Integer.parseInt(fields[3].trim().substring(0, 4));
            int month = Integer.parseInt(fields[3].trim().substring(5, 7));
            int day = Integer.parseInt(fields[3].trim().substring(8));

            Date dob = new Date(year - 1900, month - 1, day);
            row.put("dob", dob);

            double gpa = Double.parseDouble(fields[4].trim());

            row.put("gpa", gpa);

            dbApp.insertIntoTable("students", row);
            row.clear();
            if (limit != -1) {
                c--;
            }
        }
        studentsTable.close();
    }
    private static void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader transcriptsTable = new BufferedReader(new FileReader("src/main/resources/transcripts_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = transcriptsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("gpa", Double.parseDouble(fields[0].trim()));
            row.put("student_id", fields[1].trim());
            row.put("course_name", fields[2].trim());

            String date = fields[3].trim();
            int year = Integer.parseInt(date.substring(0, 4));
            int month = Integer.parseInt(date.substring(5, 7));
            int day = Integer.parseInt(date.substring(8));

            Date dateUsed = new Date(year - 1900, month - 1, day);
            row.put("date_passed", dateUsed);

            dbApp.insertIntoTable("transcripts", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        transcriptsTable.close();
    }
    private static void insertPCsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader pcsTable = new BufferedReader(new FileReader("src/main/resources/pcs_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = pcsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("pc_id", Integer.parseInt(fields[0].trim()));
            row.put("student_id", fields[1].trim());

            dbApp.insertIntoTable("pcs", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        pcsTable.close();
    }
    private static void createTranscriptsTable(DBApp dbApp) throws Exception {
        // Double CK
        String tableName = "transcripts";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameType.put("student_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("date_passed", "java.util.Date");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("gpa", "0.7");
        minValues.put("student_id", "43-0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("date_passed", "1990-01-01");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("gpa", "5.0");
        maxValues.put("student_id", "99-9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("date_passed", "2020-12-31");

        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
    }

    private static void createStudentTable(DBApp dbApp) throws Exception {
        // String CK
        String tableName = "students";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("first_name", "java.lang.String");
        htblColNameType.put("last_name", "java.lang.String");
        htblColNameType.put("dob", "java.util.Date");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("id", "43-0000");
        minValues.put("first_name", "AAAAAA");
        minValues.put("last_name", "AAAAAA");
        minValues.put("dob", "1990-01-01");
        minValues.put("gpa", "0.7");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("id", "99-9999");
        maxValues.put("first_name", "zzzzzz");
        maxValues.put("last_name", "zzzzzz");
        maxValues.put("dob", "2000-12-31");
        maxValues.put("gpa", "5.0");

        dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
    }
    private static void createPCsTable(DBApp dbApp) throws Exception {
        // Integer CK
        String tableName = "pcs";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("pc_id", "java.lang.Integer");
        htblColNameType.put("student_id", "java.lang.String");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("pc_id", "0");
        minValues.put("student_id", "43-0000");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("pc_id", "20000");
        maxValues.put("student_id", "99-9999");

        dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
    }
    private static void createCoursesTable(DBApp dbApp) throws Exception {
        // Date CK
        String tableName = "courses";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("date_added", "java.util.Date");
        htblColNameType.put("course_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("hours", "java.lang.Integer");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("date_added", "1901-01-01");
        minValues.put("course_id", "0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("hours", "1");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("date_added", "2020-12-31");
        maxValues.put("course_id", "9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("hours", "24");

        dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

    }
    public void testWrongStudentsKeyInsertion() {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "students";
        Hashtable<String, Object> row = new Hashtable();
        row.put("id", 123);

        row.put("first_name", "foo");
        row.put("last_name", "bar");

        Date dob = new Date(1995 - 1900, 4 - 1, 1);
        row.put("dob", dob);
        row.put("gpa", 1.1);

        Assertions.assertThrows(DBAppException.class, () -> {
                    dbApp.insertIntoTable(table, row);
                }
        );

    }
    public void testExtraTranscriptsInsertion() {
        final DBApp dbApp = new DBApp();
        dbApp.init();

        String table = "transcripts";
        Hashtable<String, Object> row = new Hashtable();
        row.put("gpa", 1.5);
        row.put("student_id", "34-9874");
        row.put("course_name", "bar");
        row.put("elective", true);


        Date date_passed = new Date(2011 - 1900, 4 - 1, 1);
        row.put("date_passed", date_passed);


        Assertions.assertThrows(DBAppException.class, () -> {
                    dbApp.insertIntoTable(table, row);
                }
        );
    }
    public static void main(String[] args) throws Exception {
        DBApp db = new DBApp();

//        SQLTerm[] arrSQLTerms;
//	        arrSQLTerms = new SQLTerm[2];
//	        arrSQLTerms[0] = new SQLTerm();
//	        arrSQLTerms[0]._strTableName = "students";
//	        arrSQLTerms[0]._strColumnName= "first_name";
//	        arrSQLTerms[0]._strOperator = "=";
//	        arrSQLTerms[0]._objValue =row.get("first_name");
//
//	        arrSQLTerms[1] = new SQLTerm();
//	        arrSQLTerms[1]._strTableName = "students";
//	        arrSQLTerms[1]._strColumnName= "gpa";
//	        arrSQLTerms[1]._strOperator = "<=";
//	        arrSQLTerms[1]._objValue = row.get("gpa");
//
//	        String[]strarrOperators = new String[1];
//	        strarrOperators[0] = "OR";
//	      String table = "students";
//
//	        row.put("first_name", "fooooo");
//	        row.put("last_name", "baaaar");
//
//	        Date dob = new Date(1992 - 1900, 9 - 1, 8);
//	        row.put("dob", dob);
//	        row.put("gpa", 1.1);
//
//	        dbApp.updateTable(table, clusteringKey, row);
//        createCoursesTable(db);
//        createPCsTable(db);
//        createTranscriptsTable(db);
//        insertPCsRecords(db,200);
//        insertTranscriptsRecords(db,200);



//        insertCoursesRecords(db,200);
//        db.testExtraTranscriptsInsertion();

//        Table t = getTable("students");
//
//        Hashtable<String, Object> h = new Hashtable<>();
//        h.put("id", new String("43-3542"));
//        h.put("first_name", "opWXmZ");
//        h.put("gpa", 4.34);
////        h.put("job", new Double(1.1));
//        h.put("date_added", new Date(1996-1900, 8-1, 4));
//        h.put("date_added", new Date(1996-1900, 8-1, 4));
//        System.out.println(db.printTable("students"));
//        db.deleteFromTable("students",   h);

//        db.serilizeTable("students",t);
//        db.createIndex("students", new String[]{"id", "first_name", "gpa"});
//        System.out.println(db.printTable("courses"));
//
//        createStudentTable(db);
//        insertStudentRecords(db,200);
        StringBuffer sb = new StringBuffer();
//        sb.append("INSERT INTO students (id,first_name,gpa) VALUES(\"52-9972\",\"Ahmed Wael\",3.0);");
//        sb.append("delete FROM students where id =\"52-9972\"");// AND name = \"ahmed\"");

        sb.append("UPDATE students SET gpa = 4.0 , first_name=\"ahmed wael\" , id =\"52-9972\" WHERE id = \"52-9972\"");
        sb.append("SELECT * FROM students WHERE id = \"52-9972\"");
////        sb.append("Create INDEX  index1 ON STUDENT (age,name,gpa)");
////        sb.append("CREATE TABLE student (id int PRIMARY KEY,name varchar(20),gpa double);");
//        //creating table
//        String strTableName = "student";
////        Hashtable<String, String> min = new Hashtable<>();
////        min.put("id", "0");
////        min.put("name", "A");
////        min.put("gpa", "0.0");
////        Hashtable<String, String> max = new Hashtable<>();
////        max.put("id", "1000");
////        max.put("name", "zzzzzzzzzzzzz");
////        max.put("gpa", "4.0");
////        Hashtable<String, String> htblColNameType = new Hashtable<>();
////        htblColNameType.put("id", "java.lang.Integer");
////        htblColNameType.put("name", "java.lang.String");
////        htblColNameType.put("gpa", "java.lang.Double");
////        db.createTable(strTableName, "id", htblColNameType, min, max);
//
//
        db.parseSQL(sb);
//        System.out.println(db.printTable("students"));
//        SQLTerm[] arr =new SQLTerm[2];
//
//        arr[0]=new SQLTerm();
//        arr[0]._strTableName ="courses";
//        arr[0]._strColumnName="course_id";
//        arr[0]._objValue="0950";
//        arr[0]._strOperator="!=";
//
//        arr[1]=new SQLTerm();
//        arr[1]._strTableName ="courses";
//        arr[1]._strColumnName="hours";
//        arr[1]._objValue=20;
//        arr[1]._strOperator=">";

//        arr[2]=new SQLTerm();
//        arr[2]._strTableName ="courses";
//        arr[2]._strColumnName="date_added";
//        arr[2]._objValue=new Date(2008-1900,6-1,31);
//        arr[2]._strOperator="=";

//        String[] star ={"AND"};
//        Iterator i =db.selectFromTable(arr,star);
////
//        for (Iterator it = i; it.hasNext(); ) {
//            Object o = it.next();
//            System.out.println(o);
//        }


//        db.createIndex("courses", new String[] {"course_id", "hours", "date_added"});
//        Table t =getTable("students");
//        for(Map.Entry<String,Node> m : t.getTableIndices().entrySet()) {
//            m.getValue().printComplete();
//        }

//        for (int i = 200; i < 400; i++) {
//        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
//        htblColNameValue.put("id", i);
//        htblColNameValue.put("name", "Ahmed" + i);
//        htblColNameValue.put("gpa", 4.0);
//        db.insertIntoTable(strTableName, htblColNameValue);
//        }

//        System.out.println(db.printTable(strTableName));
    }

    public int getTableLength(String tableName) throws DBAppException {
        Table table = getTable(tableName);
        int res = table.getSize();
        return res;
    }

    public static Table getTable(String tableName) throws DBAppException {
        try {
            tableName = tableName.toLowerCase();
//            System.out.println(currentConfigFile.getParent() + File.separator + tableName + File.separator + tableName + ".ser");
            FileInputStream file = new FileInputStream(currentConfigFile.getParent() + File.separator + tableName + File.separator + tableName + ".ser");
            ObjectInputStream in = new ObjectInputStream(file);

            Table table = (Table) in.readObject();

            in.close();
            file.close();
            return table;
        } catch (Exception ignored) {
            throw new DBAppException("Problem loading table");
        }
    }
}
