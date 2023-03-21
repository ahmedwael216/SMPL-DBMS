package DB;

import java.awt.*;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;
import java.lang.reflect.*;

public class Record implements Cloneable {
    private DBVector<Object> tupleRow;


    public Record(Hashtable <String,String> schema) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        tupleRow = new DBVector<Object>();
        for(String fieldName: schema.keySet()) tupleRow.add(emptyInstanceFromClass(schema.get(fieldName)));
    }
    public Record(DBVector<Object>  schema){
        tupleRow = schema;
    }


    private DBVector<Object> getTupleRow() {
        return tupleRow;
    }



    public Object getItem(int i) {
        return tupleRow.get(i);
    }
    public Object setItem(int i,Object val) {
        return tupleRow.set(i,val);
    }

    public static Object emptyInstanceFromClass(String ClassName)  {
        switch (ClassName){
            case "java.lang.Integer": return new Integer(0);
            case  "java.lang.Double": return new Double(0);
            case "java.lang.String": return new String();
            case "java.util.Date":return new Date();
            default: return new Object();
        }
    }

    protected Object clone() throws CloneNotSupportedException {
        DBVector<Object> schema = (DBVector<Object>) this.getTupleRow().clone();
        return new Record(schema);
    }

    @Override
    public String toString() {
        return "Record{" +
                "tupleRow=" + tupleRow +
                '}';
    }
}
