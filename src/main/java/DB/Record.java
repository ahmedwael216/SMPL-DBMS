package DB;

import java.util.Date;
import java.util.Hashtable;

public class Record implements Cloneable, Comparable {
    private DBVector<Comparable> tupleRow;


    public Record(String ClusteringKey, Hashtable <String,String> schema){
        tupleRow = new DBVector<Comparable>();
        tupleRow.add(emptyInstanceFromClass(schema.get(ClusteringKey)));
        schema.remove(ClusteringKey);
        for(String fieldName: schema.keySet()) tupleRow.add(emptyInstanceFromClass(schema.get(fieldName)));
    }
    public Record(DBVector<Comparable>  schema){
        tupleRow = schema;
    }

    public DBVector getDBVector(){
        return tupleRow;
    }

    private DBVector<Comparable> getTupleRow() {
        return tupleRow;
    }



    public Comparable getItem(int i) {
        return tupleRow.get(i);
    }

    public Comparable getPrimaryKey() {
        return this.getItem(0);
    }

    public Comparable setItem(int i,Comparable val) {
        return tupleRow.set(i,val);
    }

    public static Comparable emptyInstanceFromClass(String ClassName)  {
        switch (ClassName){
            case "java.lang.Integer": return new Integer(0);
            case  "java.lang.Double": return new Double(0);
            case "java.lang.String": return new String();
            case "java.util.Date":return new Date();
            default: return new Comparable() {
                @Override
                public int compareTo(Object o) {
                    return 0;
                }
            };
        }
    }

    protected Object clone() throws CloneNotSupportedException {
        DBVector<Comparable> schema = (DBVector<Comparable>) this.getTupleRow().clone();
        return new Record(schema);
    }



    @Override
    public String toString() {
        return "Record{" +
                "tupleRow=" + tupleRow +
                '}';
    }

    @Override
    public int compareTo(Object o) {
        return this.getItem(0).compareTo(((Record) o).getItem(0));
    }
}
