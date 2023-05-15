package DB;

import java.io.*;
import java.util.LinkedList;

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

    public static int findPageNumber(int n, String tableName, Comparable pk) throws IOException, ClassNotFoundException {
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

    public static void serialize(Page p, String tableName, int pageIndex) {
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

    public static Page deserialize(int x, String tableName) throws IOException, ClassNotFoundException {
        String filename = DBApp.currentDBFile + File.separator + tableName + File.separator + x + ".ser";
        FileInputStream file = new FileInputStream(filename);
        ObjectInputStream in = new ObjectInputStream(file);

        // Method for deserialization of object
        Page p = (Page) in.readObject();

        in.close();
        file.close();
        return p;
    }

    private static int deleteLinear(Record r, String tableName) throws IOException, ClassNotFoundException, DBAppException {
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

    private static void deletePage(String tableName, int pageIndex, int totalPages) throws IOException, ClassNotFoundException {
        for (int i = pageIndex + 1; i < totalPages; i++) {
            Page curr = deserialize(i, tableName);
            serialize(curr, tableName, i - 1);
        }
        setNumberOfPagesForTable(tableName, totalPages - 1);
    }

    public static int delete(String tableName, Record record) throws DBAppException, IOException, ClassNotFoundException {
        if (record.getPrimaryKey() == null) {
            return deleteLinear(record, tableName);
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
        return del;
    }

    public static void update(String tableName, Record record) throws DBAppException, IOException, ClassNotFoundException {
        int n = getNumberOfPagesForTable(tableName);
        if (n == 0) {
            throw new DBAppException("Table is empty");
        }
        int pageIndex = findPageNumber(n, tableName, (Comparable) record.getPrimaryKey());
        Page p = deserialize(pageIndex, tableName);
        p.updateRecord(record);
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
            pages.add(deserialize(i, tableName).printWithLength(max));
        }

        StringBuilder upDashes= new StringBuilder("┌");
        StringBuilder dashes = new StringBuilder("├");
        StringBuilder downDashes = new StringBuilder("└");

        for (int i = 0; i < max.length; i++) {
            int x = max[i];
            upDashes.append("─".repeat(x));
            dashes.append("─".repeat(x));
            downDashes.append("─".repeat(x));
            if(i!=max.length-1){
                upDashes.append("┬");
                dashes.append("┼");
                downDashes.append("┴");
            }
        }
        upDashes.append("┐\n");
        dashes.append("┤\n");
        downDashes.append("┘\n");


        for (String page : pages){
            s.append(upDashes);
            String[] lines = page.split("\n");
            for (int i = 0; i < lines.length; i++) {
                s.append("│").append(lines[i]).append("│\n");
                if(i!=lines.length-1){
                    s.append(dashes);
                }else{
                    s.append(downDashes);
                }
            }
        }
        return s.toString();
    }
}
