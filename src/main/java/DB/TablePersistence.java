package DB;

import java.io.*;

public class TablePersistence {
    public static int getNumberOfPagesForTable(String name) {
        return Table.getNumberOfPagesForTable(name);
    }

    public static void setNumberOfPagesForTable(String name, int x) {
        Table.setNumberOfPagesForTable(name, x);
    }

    public static void insert(String tableName, Record r) throws DBAppException, IOException, ClassNotFoundException {
        int n = getNumberOfPagesForTable(tableName);
        if (n == 0) {
            Page p = new Page();
            p.insertRecord(r);
            serialize(p, tableName, 0);
            setNumberOfPagesForTable(tableName, 1);
            return;
        }
        int pageIndex = findPageNumber(n, tableName, (Comparable) r.getPrimaryKey());
        Page p = deserialize(pageIndex, tableName);
        Record overflow = p.insertRecord(r);
        serialize(p, tableName, pageIndex);

        while (overflow != null) {
            pageIndex++;
            if (pageExists(tableName, pageIndex)) {
                Page nextP = deserialize(pageIndex, tableName);
                overflow = nextP.insertRecord(overflow);
                serialize(nextP, tableName, pageIndex);
                if (overflow == null) {
                    return;
                }
            } else {
                setNumberOfPagesForTable(tableName, pageIndex + 1);
                Page newPage = new Page();
                overflow = newPage.insertRecord(overflow);
                serialize(newPage, tableName, pageIndex);
            }
        }
    }

    private static boolean pageExists(String tableName, int pageIndex) {
        int tablePages = getNumberOfPagesForTable(tableName);
        if (pageIndex >= tablePages)
            return false;
        return true;
    }

    private static int findPageNumber(int n, String tableName, Comparable pk) throws IOException, ClassNotFoundException {
        int low = 0;
        int high = n - 1;

        while (low <= high) {
            int mid = (low + high) >> 1;
            Page midPage = deserialize(mid, tableName);
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

    private static void serialize(Page p, String tableName, int pageIndex) {
        String filename = DbApp.currentDBFile + File.separator + tableName + File.separator + pageIndex + ".ser";
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

    private static Page deserialize(int x, String tableName) throws IOException, ClassNotFoundException {
        String filename = DbApp.currentDBFile + File.separator + tableName + File.separator + x + ".ser";
        FileInputStream file = new FileInputStream(filename);
        ObjectInputStream in = new ObjectInputStream(file);

        // Method for deserialization of object
        Page p = (Page) in.readObject();

        in.close();
        file.close();
        return p;
    }

    public static void delete(String tableName, Record record) throws DBAppException, IOException, ClassNotFoundException {
        int n = getNumberOfPagesForTable(tableName);
        if (n == 0) {
            throw new DBAppException("Table is empty");
        }
        int pageIndex = findPageNumber(n, tableName, (Comparable) record.getPrimaryKey());
        Page p = deserialize(pageIndex, tableName);
        p.deleteRecord(record);
        if (p.isEmpty()) {
            File f = new File(pageIndex + ".ser");
            f.delete();
            setNumberOfPagesForTable(tableName, n - 1);
        } else
            serialize(p, tableName, pageIndex);
    }

    public static void update(String tableName, Record record) throws DBAppException, IOException, ClassNotFoundException {
        int n = getNumberOfPagesForTable(tableName);
        if (n == 0) {
            throw new DBAppException("Table is empty");
        }
        int pageIndex = findPageNumber(n, tableName, (Comparable) record.getPrimaryKey());
        Page p = deserialize(pageIndex, tableName);
        p.updateRecord(record);
    }

    public static String printTable(String tableName) throws IOException, ClassNotFoundException {
        int n = getNumberOfPagesForTable(tableName);
        StringBuilder s = new StringBuilder();
        System.out.println("\t page number = " + n);
        for (int i = 0; i < n; i++) {
            s.append("----Page ").append(i).append("----\n");
            s.append(deserialize(i, tableName).toString());
        }
        return s.toString();
    }
}
