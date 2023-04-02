package DB;
import java.io.*;

public class TablePersistence {
    public static void insert(Record r) throws DBAppException, IOException, ClassNotFoundException {
        int n= 0;
        //TODO get n the number of pages for a specific table from DBApp.cpnfig
        if(n==0){
            Page p =new Page();
            p.insertRecord(r);
            serialize(p,0);
        }
        int x = findPageNumber(n,r.getPrimaryKey());
        Page p = deserialize(x);
        Record overflow = p.insertRecord(r);
        if(overflow == null){
            return;
        }else{
            while (overflow!=null){
                x=x+1;
                Page nextP =deserialize(x);
                overflow = nextP.insertRecord(r);
                //TODO handle the case where the last page is full and create a new page
            }
        }
    }
    public static int findPageNumber(int n, Comparable pk) throws IOException, ClassNotFoundException {
        int low = 0;
        int high = n - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            Page midPage = deserialize(mid);
            int cmp = ((Comparable) midPage).compareTo(pk);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        return 0;
    }
    public static void serialize(Page p, int x){
        String filename = x+".ser";
        // Serialization
        try
        {
            //Saving of object in a file
            FileOutputStream file = new FileOutputStream(filename);
            ObjectOutputStream out = new ObjectOutputStream(file);

            // Method for serialization of object
            out.writeObject(p);

            out.close();
            file.close();
        }

        catch(IOException e)
        {
            System.out.println(e.getMessage());
        }
    }

    public static Page deserialize(int x) throws IOException, ClassNotFoundException {
        String filename = x+".ser";
        FileInputStream file = new FileInputStream(filename);
        ObjectInputStream in = new ObjectInputStream(file);

        // Method for deserialization of object
        Page p = (Page) in.readObject();

        in.close();
        file.close();
        return p;
    }
}
