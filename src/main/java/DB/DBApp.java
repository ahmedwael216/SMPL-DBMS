package DB;

import java.io.*;
import java.text.ParseException;
import java.util.*;
//import java.util.Iterator;


public class DBApp {
    public static int maxRecordsCountPage;
    // Path to SMPL-DBMS
    public static String rootPath = new File(System.getProperty("user.dir")).getAbsolutePath();//.getParentFile().getParentFile().getParentFile().getParent() + File.separator;
    public static String selectedDBName = null;
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
            getMaximumRecordsCountinPage();
        } else {
            System.out.println("creating database in resources folder");

            DBVector<String> lines = new DBVector<>();
            lines.add("databaseName = " + selectedDBName);
            lines.add("maximumNumberOfRows = 200");
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

    public void getMaximumRecordsCountinPage() {
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

    // following method creates an octree
    // depending on the count of column names passed.
    // If three column names are passed, create an octree.
    // If only one or two column names is passed, throw an Exception.
    public void createIndex(String strTableName,
                            String[] strarrColName)
            throws DBAppException {

    }

    /**
     * Inserts a new row in a specified table.
     * Inserted data must have a value for the primary key.
     *
     * @param strTableName     name of the table to insert the row into
     * @param htblColNameValue maps columns' names to their corresponding values to be inserted
     * @throws DBAppException If an exception occurred
     */
    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
            throws DBAppException {
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(currentConfigFile.getAbsolutePath()));
            if (prop.getProperty(strTableName + "TablePages") != null) {
                prop.store(new FileWriter(currentConfigFile.getAbsolutePath()), "Insert into " + strTableName + " table");

                Table table = getTable(strTableName);

                table.insertIntoTable(strTableName, htblColNameValue);

                FileOutputStream fileOut = new FileOutputStream(currentConfigFile.getParent() + File.separator + strTableName + File.separator + strTableName + ".ser");
                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                out.writeObject(table);
                out.close();
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
            prop.load(new FileInputStream(currentConfigFile.getAbsolutePath()));
            if (prop.getProperty(strTableName + "TablePages") != null) {
                prop.store(new FileWriter(currentConfigFile.getAbsolutePath()), "Update " + strTableName + " table");

                Table table = getTable(strTableName);

                table.updateTable(strTableName, strClusteringKeyValue, htblColNameValue);

                FileOutputStream fileOut = new FileOutputStream(currentConfigFile.getParent() + File.separator + strTableName + File.separator + strTableName + ".ser");
                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                out.writeObject(table);
                out.close();
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
            prop.load(new FileInputStream(currentConfigFile.getAbsolutePath()));
            if (prop.getProperty(strTableName + "TablePages") != null) {
                prop.store(new FileWriter(currentConfigFile.getAbsolutePath()), "Delete From " + strTableName + " table");
                System.out.println(currentConfigFile.getParent() + File.separator + strTableName + File.separator + strTableName + ".ser");

                Table table = getTable(strTableName);

                table.deleteFromTable(strTableName, htblColNameValue);

                FileOutputStream fileOut = new FileOutputStream(currentConfigFile.getParent() + File.separator + strTableName + File.separator + strTableName + ".ser");
                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                out.writeObject(table);
                out.close();
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

    public String printTable(String strTableName) throws IOException, ClassNotFoundException {
        Table table = getTable(strTableName);
        String ret = table.toString();

        FileOutputStream fileOut = new FileOutputStream(currentConfigFile.getParent() + File.separator + strTableName + File.separator + strTableName + ".ser");
        ObjectOutputStream out = new ObjectOutputStream(fileOut);
        out.writeObject(table);
        out.close();

        return ret;
    }

    /*
     * public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
     * String[] strarrOperators)
     * throws DBAppException{}
     */
    //TODO create SQL Term class

    /**
     * Helper function to check if the column has an index or not.
     *
     * @return index if found or null if not
     */
    //public ??? findIndex(String strTableName,
    //                     String strColName)
    //                     throws DBAppException {}
    public static void main(String[] args) throws IOException, DBAppException, ClassNotFoundException {
        DBApp db = new DBApp();
        //creating table
        String strTableName = "Student";
//        Hashtable<String, String> min = new Hashtable<>();
//        min.put("id", "0");
//        min.put("name", "A");
//        min.put("gpa", "0.0");
//        Hashtable<String, String> max = new Hashtable<>();
//        max.put("id", "1000");
//        max.put("name", "zzzzzzzzzzzzz");
//        max.put("gpa", "4.0");
//        Hashtable<String, String> htblColNameType = new Hashtable<>();
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.Double");
//        db.createTable(strTableName, "id", htblColNameType, min, max);
//
//        for (int i = 200; i < 400; i++) {
//        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
//        htblColNameValue.put("id", i);
//        htblColNameValue.put("name", "Ahmed" + i);
//        htblColNameValue.put("gpa", 4.0);
//        db.insertIntoTable(strTableName, htblColNameValue);
//        }

        System.out.println(db.printTable(strTableName));
    }

    public int getTableLength(String tableName) throws IOException, ClassNotFoundException {
        Table table = getTable(tableName);
        int res = table.getSize();
        return res;
    }

    private Table getTable(String tableName) throws IOException, ClassNotFoundException {
        FileInputStream file = new FileInputStream(currentConfigFile.getParent() + File.separator + tableName + File.separator + tableName + ".ser");
        ObjectInputStream in = new ObjectInputStream(file);

        Table table = (Table) in.readObject();

        in.close();
        file.close();
        return table;
    }
}
