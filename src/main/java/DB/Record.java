package DB;

import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;

public class Record implements Cloneable, Comparable, Serializable {
    private DBVector<Serializable> tupleRow;

    public Record(String ClusteringKey, Hashtable<String, String> schema) {
        tupleRow = new DBVector<Serializable>();
        tupleRow.add(emptyInstanceFromClass(schema.get(ClusteringKey)));
        for (String fieldName : schema.keySet())
            if (!fieldName.equals(ClusteringKey))
                tupleRow.add(emptyInstanceFromClass(schema.get(fieldName)));
    }

    public Record(DBVector<Serializable> schema) {
        tupleRow = schema;
    }

    public DBVector getDBVector() {
        return tupleRow;
    }

    private DBVector<Serializable> getTupleRow() {
        return tupleRow;
    }

    public Serializable getItem(int i) {
        return tupleRow.get(i);
    }

    public Serializable getPrimaryKey() {
        return this.getItem(0);
    }

    public Serializable setItem(int i, Serializable val) {
        return tupleRow.set(i, val);
    }

    public static Serializable emptyInstanceFromClass(String ClassName) {
        ClassName = ClassName.toLowerCase();
        switch (ClassName) {
            case "java.lang.integer":
                return new Integer(0);
            case "java.lang.double":
                return new Double(0);
            case "java.lang.string":
                return new String();
            case "java.util.date":
                return new Date();
            default:
                return new Serializable() {
                };
        }
    }

    protected Object clone() throws CloneNotSupportedException {
        DBVector<Serializable> schema = (DBVector<Serializable>) this.getTupleRow().clone();
        return new Record(schema);
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        for (Serializable c : this.tupleRow) {
            if (c == null)
                s.append("null").append("| ");
            else
                s.append(c.toString()).append("| ");
        }
        return s.toString();
    }

    @Override
    public int compareTo(Object o) {
        return ((Comparable) this.getItem(0)).compareTo(((Record) o).getItem(0));
    }

    public boolean equals(Object o) {
        for (int i = 0; i < this.tupleRow.size(); i++) {
            if (this.getItem(i) == null && ((Record) o).getItem(i) == null)
                continue;
            if (this.getItem(i) == null)
                return false;
            if (((Record) o).getItem(i) == null)
                continue;
            if (!this.getItem(i).equals(((Record) o).getItem(i)))
                return false;
        }
        return true;
    }
}
