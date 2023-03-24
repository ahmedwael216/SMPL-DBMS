package DB;

import java.util.*;
//import java.util.Iterator;

public class DBApp {

    /**
     * Executes at application startup.
     * Checks if there are previously defined databases.
     * Prompts user to choose from available databases or create new one.
     */
    public void init() {
        ArrayList<String> availableDatabases = new ArrayList<>();
        // TODO find existing databases

        System.out.println("Welcome to our Database Management System!\n------------------------------------------");
        System.out.println("To create a new database, type its name.");
        if (availableDatabases.size() > 0) {
            System.out.println("To load an existing database, type its name from the following list:");
            System.out.print(availableDatabases.get(0));
            for (String databaseName : availableDatabases) {
                System.out.print(" ||| " + databaseName);
            }
        }
        System.out.print("\n\nLoad database: ");
        Scanner sc = new Scanner(System.in);
        String chosenDatabaseName = sc.nextLine();
        sc.close();
        // TODO load the chosen database
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
    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators)
            throws DBAppException{}
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
