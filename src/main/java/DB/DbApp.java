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
                availableDatabases.add(fileName);
            }
        }

        // Prompt user for database name
        System.out.println("Hello " + System.getProperty("user.name") + ", welcome to our Simple Database Management System!");
        System.out.println("Please enter the database name you would like to load.");
        // if (availableDatabases.size() > 0) {
        //     System.out.println("To load an existing database, type its name from the following list:");
        //     System.out.print(availableDatabases.get(0));
        //     for (int i = 1; i < availableDatabases.size(); i++) {
        //         System.out.print(" ||| " + availableDatabases.get(i));
        //     }
        // }
        System.out.print("\n\nLoad database: ");
        Scanner sc = new Scanner(System.in);
        String chosenDatabaseName = sc.nextLine();
        sc.close();

        // Create new database if does not exist
        if (!availableDatabases.contains(chosenDatabaseName)) {
            File newDatabase = new File(rootPath + chosenDatabaseName);
            if (newDatabase.mkdir()) {
                DBVector<String> lines = new DBVector<>();
                lines.add("Database Name = " + chosenDatabaseName);
                lines.add("MaximumRowsCountinTablePage = 200");
                File newConfig = new File(rootPath + chosenDatabaseName + "/" + chosenDatabaseName + ".config");
                try {
                    FileWriter fw = new FileWriter(newConfig);
                    for (String line : lines) {
                        fw.append(line);
                    }
                    fw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /*
     * Creates a new table in the current database.
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
        String root = currentDbPath.getAbsolutePath();
        try {
            FileInputStream is = new FileInputStream(root);
            prop.load(is);
            if (prop.getProperty(strTableName + "Pages") != null) {
                System.err.println(strTableName + "already exists in database" + currentDb);
            } else {

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
