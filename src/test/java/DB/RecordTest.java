package DB;

import com.sun.jdi.event.StepEvent;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.Hashtable;

import static org.junit.jupiter.api.Assertions.*;

class RecordTest {

    @Test
    void emptyInstanceFromClassShouldCreateInteger() {
        assertEquals((new Integer(0)).getClass(), Record.emptyInstanceFromClass("java.lang.Integer").getClass());
    }

    @Test
    void emptyInstanceFromClassShouldCreateString(){
        assertEquals((new String("")).getClass(), Record.emptyInstanceFromClass("java.lang.String").getClass());
    }

    @Test
    void emptyInstanceFromClassShouldCreateDouble() {
        assertEquals((new Double(0)).getClass(), Record.emptyInstanceFromClass("java.lang.Double").getClass());
    }

    @Test
    void emptyInstanceFromClassShouldCreateDate() {
        assertEquals((new Date()).getClass(), Record.emptyInstanceFromClass("java.util.Date").getClass());
    }

    @Test
    void constructorShouldPlaceTheClusteringKeyAtTheFirstIndex(){
        Hashtable<String,String> schema = new Hashtable<String,String>();

        schema.put("A","java.lang.String");
        schema.put("B","java.lang.Integer");
        schema.put("C","java.lang.Double");

        String ClusteringKey = "B";

        Record prototype = new Record(ClusteringKey,schema);

        assertEquals((new Integer(0)).getClass(),prototype.getItem(0).getClass());
        assertEquals((new String()).getClass(),prototype.getItem(1).getClass());
        assertEquals((new Double(0.0)).getClass(),prototype.getItem(2).getClass());

    }


}