package DB;

import java.io.*;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

public class TablePersistence {
    public static int getNumberOfPagesForTable(String name) {
        return Table.getNumberOfPagesForTable(name);
    }

    public void setNumberOfPagesForTable(String name, int x) {
        Table.setNumberOfPagesForTable(name, x);
    }

    public void insertIntoIndex(String tableName, Record record, int pageNumber) throws DBAppException {
        Table table = DBApp.getTable(tableName);
        String[] keys = table.keys;
        for (Map.Entry<String, Node> m : table.getTableIndices().entrySet()) {
            String[] indexCols = m.getKey().split(",");
            Node indexRoot = m.getValue();

            int colXIndex = SearchStrategy.getColIndex(keys, indexCols[0]);
            int colYIndex = SearchStrategy.getColIndex(keys, indexCols[1]);
            int colZIndex = SearchStrategy.getColIndex(keys, indexCols[2]);

            if (record.getItem(colXIndex) != null && record.getItem(colYIndex) != null && record.getItem(colZIndex) != null) {
                indexRoot.insert(Table.createPoint(table, record, indexCols), pageNumber);
            }
        }
    }

    public void deleteSingleRecordFromIndex(String tableName, Record record, int pageNumber) throws DBAppException {
        Table table = DBApp.getTable(tableName);
        String[] keys = table.keys;
        for (Map.Entry<String, Node> m : table.getTableIndices().entrySet()) {
            String[] indexCols = m.getKey().split(",");
            Node indexRoot = m.getValue();

            int colXIndex = SearchStrategy.getColIndex(keys, indexCols[0]);
            int colYIndex = SearchStrategy.getColIndex(keys, indexCols[1]);
            int colZIndex = SearchStrategy.getColIndex(keys, indexCols[2]);

            if (record.getItem(colXIndex) != null && record.getItem(colYIndex) != null && record.getItem(colZIndex) != null) {
                indexRoot.delete(Table.createPoint(table, record, indexCols), true, pageNumber);
            }
        }
    }

    public void insert(String tableName, Record r) throws DBAppException, IOException, ClassNotFoundException {
        int n = getNumberOfPagesForTable(tableName);

        if (n == 0) {
            Page p = new Page();
            p.insertRecord(r);
            serialize(p, tableName, 0);
            setNumberOfPagesForTable(tableName, 1);
            insertIntoIndex(tableName, r, 0);
            return;
        }
        int pageIndex = findPageNumber(n, tableName, (Comparable) r.getPrimaryKey());
        insertIntoIndex(tableName, r, pageIndex);
        Page p = deserialize(pageIndex, tableName);
        Record overflow = p.insertRecord(r);

        serialize(p, tableName, pageIndex);

        while (overflow != null) {
            deleteSingleRecordFromIndex(tableName, overflow, pageIndex);
            pageIndex++;
            if (pageExists(tableName, pageIndex)) {
                Page nextP = deserialize(pageIndex, tableName);
                insertIntoIndex(tableName, overflow, pageIndex);
                overflow = nextP.insertRecord(overflow);
                serialize(nextP, tableName, pageIndex);
                if (overflow == null) {
                    return;
                }
            } else {
                setNumberOfPagesForTable(tableName, pageIndex + 1);
                Page newPage = new Page();
                insertIntoIndex(tableName, overflow, pageIndex);
                overflow = newPage.insertRecord(overflow);
                serialize(newPage, tableName, pageIndex);
            }
        }
    }

    private boolean pageExists(String tableName, int pageIndex) {
        int tablePages = getNumberOfPagesForTable(tableName);
        if (pageIndex >= tablePages)
            return false;
        return true;
    }

    public static int findPageNumber(int n, String tableName, Comparable pk) throws IOException, ClassNotFoundException {
        int low = 0;
        int high = n - 1;

        while (low <= high) {
            int mid = (low + high) >> 1;
            TablePersistence tp = new TablePersistence();
            Page midPage = tp.deserialize(mid, tableName);
            int cmp = ((Comparable) midPage).compareTo(pk);
            if (cmp > 0)
                low = mid + 1;
            else if (cmp < 0)
                high = mid - 1;
            else
                return mid;
        }
        return 0;
    }

    public void serialize(Page p, String tableName, int pageIndex) {
        String filename = DBApp.currentDBFile + File.separator + tableName + File.separator + pageIndex + ".ser";
        // Serialization
        try {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream(filename);
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(p);

            out.close();
            file.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

    public Page deserialize(int x, String tableName) throws IOException, ClassNotFoundException {
        String filename = DBApp.currentDBFile + File.separator + tableName + File.separator + x + ".ser";
        FileInputStream file = new FileInputStream(filename);
        ObjectInputStream in = new ObjectInputStream(file);

        // Method for deserialization of object
        Page p = (Page) in.readObject();

        in.close();
        file.close();
        return p;
    }

    private int deleteLinear(Record r, String tableName) throws IOException, ClassNotFoundException, DBAppException {
        int n = getNumberOfPagesForTable(tableName);
        int totDel = 0;
        for (int i = n - 1; i >= 0; i--) {
            Page p = deserialize(i, tableName);
            totDel += p.deleteLinear(r);
            if (p.isEmpty()) {
                deletePage(tableName, i, n);
                n--;
            } else
                serialize(p, tableName, i);
        }
        return totDel;
    }

    private void deletePage(String tableName, int pageIndex, int totalPages) throws IOException, ClassNotFoundException {
        for (int i = pageIndex + 1; i < totalPages; i++) {
            Page curr = deserialize(i, tableName);
            serialize(curr, tableName, i - 1);
        }
        setNumberOfPagesForTable(tableName, totalPages - 1);
    }

    public static void deleteFromIndex(String tableName, Record record) throws DBAppException {
        Table table = DBApp.getTable(tableName);

        String[] keys = table.keys;
        for (Map.Entry<String, Node> m : table.getTableIndices().entrySet()) {
            String[] indexCols = m.getKey().split(",");
            Node indexRoot = m.getValue();

            int colXIndex = SearchStrategy.getColIndex(keys, indexCols[0]);
            int colYIndex = SearchStrategy.getColIndex(keys, indexCols[1]);
            int colZIndex = SearchStrategy.getColIndex(keys, indexCols[2]);

            if (record.getItem(colXIndex) != null && record.getItem(colYIndex) != null && record.getItem(colZIndex) != null) {
                indexRoot.delete(Table.createPoint(table, record, indexCols), false, -1);
            }
        }
    }

    public int delete(String tableName, Record record) throws DBAppException, IOException, ClassNotFoundException {
        if (record.getPrimaryKey() == null) {

            Table table = DBApp.getTable(tableName);

            String[] keys = table.keys;
            if (table.getTableIndices().size() == 0) {
                return deleteLinear(record, tableName);
            } else {
                int ret = 0;
                for (Map.Entry<String, Node> m : table.getTableIndices().entrySet()) {
                    String[] indexCols = m.getKey().split(",");
                    Node indexRoot = m.getValue();

                    int colXIndex = SearchStrategy.getColIndex(keys, indexCols[0]);
                    int colYIndex = SearchStrategy.getColIndex(keys, indexCols[1]);
                    int colZIndex = SearchStrategy.getColIndex(keys, indexCols[2]);

                    if (record.getItem(colXIndex) != null && record.getItem(colYIndex) != null && record.getItem(colZIndex) != null) {
                        DimRange xRange = new DimRange((Comparable) record.getItem(colXIndex), (Comparable) record.getItem(colXIndex));
                        DimRange yRange = new DimRange((Comparable) record.getItem(colYIndex), (Comparable) record.getItem(colYIndex));
                        DimRange zRange = new DimRange((Comparable) record.getItem(colZIndex), (Comparable) record.getItem(colZIndex));

                        DBVector<Integer> pageIndices = indexRoot.search(xRange, yRange, zRange, true, true, true, true, true, true);

                        int n = getNumberOfPagesForTable(tableName);
                        int totDel = 0;
                        for (int pageIdx : pageIndices) {
                            Page p = deserialize(pageIdx, tableName);
                            totDel += p.deleteLinear(record);
                            if (p.isEmpty()) {
                                deletePage(tableName, pageIdx, n);
                                n--;
                            } else
                                serialize(p, tableName, pageIdx);
                        }
                        ret += totDel;
                    }
                }
                deleteFromIndex(tableName, record);
                return ret;
            }
        }


        int n = getNumberOfPagesForTable(tableName);
        if (n == 0) {
            throw new DBAppException("Table is empty");
        }
        int pageIndex = findPageNumber(n, tableName, (Comparable) record.getPrimaryKey());
        Page p = deserialize(pageIndex, tableName);
        int del = p.deleteRecord(record);

        if (p.isEmpty()) {
            deletePage(tableName, pageIndex, n);
        } else
            serialize(p, tableName, pageIndex);

        deleteFromIndex(tableName, record);
        return del;
    }

    public void update(String tableName, Record record) throws DBAppException, IOException, ClassNotFoundException, CloneNotSupportedException {
        Table table = DBApp.getTable(tableName);

        int n = getNumberOfPagesForTable(tableName);
        if (n == 0) {
            throw new DBAppException("Table is empty");
        }

        int pageIndex = findPageNumber(n, tableName, (Comparable) record.getPrimaryKey());
        Page p = deserialize(pageIndex, tableName);


        SQLTerm getRecord = new SQLTerm();
        getRecord._strColumnName = table.keys[0];
        getRecord._strOperator = "=";
        getRecord._strTableName = table.getName();
        getRecord._objValue = (Comparable) record.getPrimaryKey();

        DBVector<Record> queryResult = ClusteringKeySearch.Search(getRecord, table.keys, table.prototype);

        Record removeRecord = queryResult.get(0);


        for (Map.Entry<String, Node> m : table.getTableIndices().entrySet()) {
            String[] indexCols = m.getKey().split(",");
            Node index = m.getValue();
            Point3D point = Table.createPoint(table, removeRecord, indexCols);
            index.delete(point, true, pageIndex);
        }

        p.updateRecord(record);

        for (Map.Entry<String, Node> m : table.getTableIndices().entrySet()) {
            String[] indexCols = m.getKey().split(",");
            Node index = m.getValue();
            Point3D point = Table.createPoint(table, record, indexCols);
            index.update(point, pageIndex);
        }

        serialize(p, tableName, pageIndex);
    }

    public static String printTable(String tableName) throws IOException, ClassNotFoundException {
        FileInputStream file = new FileInputStream(DBApp.currentConfigFile.getParent() + File.separator + tableName + File.separator + tableName + ".ser");
        ObjectInputStream in = new ObjectInputStream(file);
        Table table = (Table) in.readObject();
        in.close();
        file.close();
        int[] max = table.getAllMaxValuesString();

        int n = getNumberOfPagesForTable(tableName);
        StringBuilder s = new StringBuilder();
        LinkedList<String> pages = new LinkedList<>();
        for (int i = 0; i < n; i++) {
            TablePersistence tp = new TablePersistence();
            pages.add(tp.deserialize(i, tableName).printWithLength(max));
        }

        StringBuilder upDashes = new StringBuilder("┌");
        StringBuilder dashes = new StringBuilder("├");
        StringBuilder downDashes = new StringBuilder("└");

        for (int i = 0; i < max.length; i++) {
            int x = max[i];
            upDashes.append("─".repeat(x));
            dashes.append("─".repeat(x));
            downDashes.append("─".repeat(x));
            if (i != max.length - 1) {
                upDashes.append("┬");
                dashes.append("┼");
                downDashes.append("┴");
            }
        }
        upDashes.append("┐\n");
        dashes.append("┤\n");
        downDashes.append("┘\n");

        for (String page : pages) {
            s.append(upDashes);
            String[] lines = page.split("\n");
            for (int i = 0; i < lines.length; i++) {
                s.append("│").append(lines[i]).append("│\n");
                if (i != lines.length - 1) {
                    s.append(dashes);
                } else {
                    s.append(downDashes);
                }
            }
        }
        return s.toString();
    }

    public static Iterator select(String name, SQLTerm[] arrSQLTerms, String[] strarrOperators) {

        return null;
    }
}
