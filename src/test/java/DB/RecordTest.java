package DB;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class RecordTest {

    @Test
    void emptyInstanceFromClassShouldCreateInteger() throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        assertEquals((new Integer(0)).getClass(), Record.emptyInstanceFromClass("java.lang.Integer").getClass());
    }

    @Test
    void emptyInstanceFromClassShouldCreateString() throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        assertEquals((new String("")).getClass(), Record.emptyInstanceFromClass("java.lang.String").getClass());
    }

    @Test
    void emptyInstanceFromClassShouldCreateDouble() throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        assertEquals((new Double(0)).getClass(), Record.emptyInstanceFromClass("java.lang.Double").getClass());
    }

    @Test
    void emptyInstanceFromClassShouldCreateDate() throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        assertEquals((new Date()).getClass(), Record.emptyInstanceFromClass("java.util.Date").getClass());
    }

}