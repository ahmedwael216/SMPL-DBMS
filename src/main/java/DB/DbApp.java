package DB;

import DB.DBVector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Scanner;
//import java.util.Iterator;


public class DbApp {
    public static int maxRecordsCountPage;
    String rootPath = new File(System.getProperty("user.dir")).getParentFile().getParentFile().getParentFile().getParent() + File.separator;
     public String selectedDB = null;
     public File currentDB = null;

    /**
     * Executes at application startup.
     * Prompts user to enter absolute path of database to use.
     *
     * @throws FielNotFoundException    if an error occurred while manipulating files
     * @throws IOException              if an error occurred while inputting or outputting
     */
    public void init() throws FileNotFoundException, IOException {
        // Store currently available database names
        DBVector<String> availableDatabases = new DBVector<>();
        DBVector<String> excludedDirs = new DBVector<>();
        Collections.addAll(excludedDirs, ".metadata", ".git", "src", "target");
        File rootFile = new File(rootPath);
        String rootContents[] = rootFile.list();
        for (String fileName : rootContents) {
            File file = new File(rootPath + File.separator + fileName);
            if (file.isDirectory() && !excludedDirs.contains(fileName)) {
                availableDatabases.add(fileName.toLowerCase());
            }
        }

        // Prompt user for database name
        System.out.println("\nHello " + System.getProperty("user.name") + ", welcome to our Simple Database Management System!");
        System.out.println("Please enter the database name you would like to load.");
        System.out.println("Allowed characters: A-Z, 0-9, underscore (case in-sensitive)\n\n");
        Scanner sc = new Scanner(System.in);
        do {
            System.out.print("Load database: ");
            selectedDB = sc.nextLine();
        } while(selectedDB == null || !selectedDB.matches("^[a-zA-Z0-9_]+"));
        sc.close();

        // Moving to selected database
        if (availableDatabases.contains(selectedDB.toLowerCase())) {
            System.out.println("Switching context to exisitng database: " + selectedDB);
        } else {
            System.out.println("Creating new database: " + selectedDB);
            File newDatabase = new File(rootPath + File.separator + selectedDB);
            if (newDatabase.mkdir()) {
                DBVector<String> lines = new DBVector<>();
                lines.add("databaseName = " + selectedDB);
                lines.add("maximumNumberOfRows = 200");
                File newConfig = new File(rootPath + File.separator + selectedDB + File.separator + "DB.config");
                try {
                    FileWriter fw = new FileWriter(newConfig);
                    for (String line : lines) {
                        fw.append(line + "\n");
                    }
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        currentDB = new File(rootPath + File.separator + selectedDB);
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
        String configPath = currentDB.getAbsolutePath() + File.separator + "DB.config";
        try {
            FileInputStream is = new FileInputStream(configPath);
            prop.load(is);
            if (prop.getProperty(strTableName + "TablePages") != null) {
                System.err.println("The table \"" + strTableName + "\" already exists in the database \"" + selectedDB + "\"");
            } else {
                prop.setProperty(strTableName + "TablePages", "0");
                prop.store(new FileWriter(configPath), "Created " + strTableName + " table");
                System.out.println("The table \"" + strTableName + "\" has been added to the database \"" + selectedDB + "\"");
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getMaximumRecordsCountinPage() {
        Properties prop = new Properties();
        String fileName = "src/main/java/DB/config/DBApp.config";
        try {
            FileInputStream is = new FileInputStream(fileName);
            prop.load(is);
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
                            throws DBAppException{

    }

    /**
     * Inserts a new row in a specified table.
     * Inserted data must have a value for the primary key.
     *
     * @param strTableName              name of the table to insert the row into
     * @param htblColNameValue          maps columns' names to their corresponding values to be inserted
     *
     * @throws DBAppException           If an exception occurred
     */
    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
                                throws DBAppException {

    }

    /**
     * Updates a row in a specified table.
     *
     * @param strTableName              name of the table to update the row from
     * @param strClusteringKeyValue     the value to look for to find the row to update
     * @param htblColNameValue          maps the column names and their corresponding new values to be updated.
     *                                  Cannot include clustering key as column name.
     *
     * @throws DBAppException           If an exception occurred
     */
    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue)
                            throws DBAppException {

    }

    /**
     * Delete one or more rows from a specified table.
     * Searches for the values in the specified columns and deletes matching row(s).
     * Entries are anded together.
     *
     * @param strTableName                  name of the table to delete row(s) from
     * @param htblColNameValue              maps column names to certain value and delete the row(s) if matching(s) found
     *
     * @throws DBAppException           If an exception occurred
     */
    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue)
                                throws DBAppException {

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
    // TODO
    //public ??? findIndex(String strTableName,
    //                     String strColName)
    //                     throws DBAppException {}
}
