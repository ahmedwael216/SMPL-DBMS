package DB;
import java.io.*;
import java.util.Properties;

public class TablePersistence {
    public static int getNumberOfPagesForTable(String name) {
        Properties prop = new Properties();
        String fileName = "src/main/java/DB/config/DBApp.config";
        try {
            FileInputStream is = new FileInputStream(fileName);
            prop.load(is);
            return  Integer.parseInt(prop.getProperty("NumberOfPagesOfTable"+name));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
    public static void setNumberOfPagesForTable(String name, int x) {
        Properties prop = new Properties();
        String fileName = "src/main/java/DB/config/DBApp.config";
        try {
            FileInputStream is = new FileInputStream(fileName);
            prop.load(is);
            prop.setProperty("NumberOfPagesOfTable"+name, x+"");
            FileOutputStream os = new FileOutputStream(fileName);
            prop.store(os,null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static void insert(String name , Record r) throws DBAppException, IOException, ClassNotFoundException {
        int n= getNumberOfPagesForTable(name);
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
                if(pageExists(x)){
                    Page nextP =deserialize(x);
                    overflow = nextP.insertRecord(r);
                }else{
                    setNumberOfPagesForTable(name,x);
                    Page newPage = new Page();
                    newPage.insertRecord(overflow);
                    serialize(newPage,x);
                }
            }
        }
    }
    public static boolean pageExists(int x){
        String filename = x+".ser";
        //TODO check for file existence
        return true;
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
