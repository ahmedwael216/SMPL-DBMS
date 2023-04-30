package DB;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class DBVectorTest {
    /**
     * Method under test: {@link DBVector#clone()}
     */
    @Test
    void testClone() {
        assertTrue(((DBVector<Object>) (new DBVector<>()).clone()).isEmpty());
    }

    /**
     * Method under test: {@link DBVector#binarySearch(Object)}
     */
    @Test
    void testBinarySearch() {
        assertEquals(-1, (new DBVector<>()).binarySearch("Record"));
    }

    /**
     * Method under test: {@link DBVector#binarySearch(Object)}
     */
    @Test
    void testBinarySearch2() {
        DBVector<Object> objectList = new DBVector<>();
        objectList.add("a");
        assertEquals(-2, objectList.binarySearch("b"));
    }

    /**
     * Method under test: {@link DBVector#binarySearch(Object)}
     */
    @Test
    void testBinarySearch3() {
        DBVector<Object> objectList = new DBVector<>();
        objectList.add("b");
        assertEquals(-1, objectList.binarySearch("a"));
    }

    /**
     * Method under test: {@link DBVector#binarySearch(Object)}
     */
    @Test
    @Disabled("TODO: Complete this test")
    void testBinarySearch4() {
        //   Reason: R013 No inputs found that don't throw a trivial exception.
        //   Diffblue Cover tried to run the arrange/act section, but the method under
        //   test threw
        //   java.lang.NullPointerException
        //       at DB.DBVector.binarySearch(DBVector.java:17)
        //   See https://diff.blue/R013 to resolve this issue.

        DBVector<Object> objectList = new DBVector<>();
        objectList.add(null);
        objectList.binarySearch("Record");
    }

    /**
     * Method under test: {@link DBVector#binarySearch(Object)}
     */
    @Test
    void testBinarySearch5() {
        DBVector<Object> objectList = new DBVector<>();
        objectList.add(true);
        assertEquals(0, objectList.binarySearch(true));
    }
}

