package DB;

import java.awt.*;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;
import java.lang.reflect.*;

public class Record implements Cloneable {
    Vector<Object> tupleRow;
    public Record(Hashtable <String,String> schema) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        tupleRow = new Vector<Object>();
        for(String fieldName: schema.keySet()) tupleRow.add(emptyInstanceFromClass(schema.get(fieldName)));
    }

    public static Object emptyInstanceFromClass(String ClassName) throws InvocationTargetException, InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException {
        String fieldDataType = ClassName;
        Class DataTypeClass = Class.forName(fieldDataType);
        return ((fieldDataType=="java.lang.Integer" || fieldDataType == "java.lang.Double")?
                0:
                DataTypeClass.getConstructor().newInstance());
    }

    public static void main(String [] args) throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
    }

    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

}
