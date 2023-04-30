package DB;

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DBAppTest {
    static DBApp DbApp;
    static Hashtable<String,String> htblColNameType;
    static Hashtable<String,String> min;
    static Hashtable<String,String> max;


    @BeforeAll
    static void setUp() {
        DbApp = new DBApp();

        htblColNameType = new Hashtable<>();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        htblColNameType.put("birthday", "java.util.Date");

        min = new Hashtable<>();
        min.put("id", "0");
        min.put("name", "A");
        min.put("gpa", "0.0");
        min.put("birthday", "1990-01-01");

        max = new Hashtable<>();
        max.put("id", "1000");
        max.put("name", "zzzzzzzzzzzzz");
        max.put("gpa", "4.0");
        max.put("birthday", "2023-04-30");
    }

    @Test
    @Order(1)
//    @Disabled()
    void createTableWithPrimaryKeyInt() throws DBAppException {
        String strTableName = "StudentInt";
        DbApp.createTable(strTableName, "id", htblColNameType, min, max);
    }
    @Test
    @Order(2)
//    @Disabled()
    void createTableWithPrimaryKeyDouble() throws DBAppException {
        String strTableName = "StudentDouble";
        DbApp.createTable(strTableName, "gpa", htblColNameType, min, max);
    }
    @Test
    @Order(3)
//    @Disabled()
    void createTableWithPrimaryKeyString() throws DBAppException {
        String strTableName = "StudentString";
        DbApp.createTable(strTableName, "name", htblColNameType, min, max);

    }
    @Test
    @Order(4)
//    @Disabled()
    void createTableWithPrimaryKeyDate() throws DBAppException {
        String strTableName = "StudentDate";
        DbApp.createTable(strTableName, "birthday", htblColNameType, min, max);

    }


    @Test
    @Order(5)
    void insertIntoTableIntNewRow() throws DBAppException, ParseException {
        String strTableName = "StudentInt";
        for (int i = 0; i < 400; i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", i );
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 4.0);
            SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD");
            htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        }
    }
    @Test
    @Order(6)
    void insertIntoTableDoubleNewRow() throws DBAppException, ParseException {
        String strTableName = "StudentDouble";
        for (int i = 0; i <= 4; i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", i );
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", (double) i);
            SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD");
            htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        }
    }
    @Test
    @Order(7)
    void insertIntoTableStringNewRow() throws DBAppException, ParseException {
        String strTableName = "StudentString";
        for (int i = 0; i < 400; i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", i );
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 4.0);
            SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD");
            htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        }
    }
    @Test
    @Order(8)
    void insertIntoTableDateNewRow() throws DBAppException, ParseException {
        String strTableName = "StudentDate";
        for (int i = 1; i <= 9; i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", i );
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 4.0);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            htblColNameValue.put("birthday", formatter.parse("2000-01-0" + i));
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        }

    }

    @Test
    @Order(9)
    void checkTableLengthForTableInt() throws IOException, ClassNotFoundException {
        Assertions.assertEquals(400,DbApp.getTableLength("StudentInt"));
    }
    @Test
    @Order(10)
    void checkTableLengthForTableDouble() throws IOException, ClassNotFoundException {
        Assertions.assertEquals(5,DbApp.getTableLength("StudentDouble"));
    }
    @Test
    @Order(11)
    void checkTableLengthForTableString() throws IOException, ClassNotFoundException {
        Assertions.assertEquals(400,DbApp.getTableLength("StudentString"));
    }
    @Test
    @Order(12)
    void checkTableLengthForTableDate() throws IOException, ClassNotFoundException {
        Assertions.assertEquals(9,DbApp.getTableLength("StudentDate"));
    }

    @Test
    @Order(13)
    void insertIntoTableIntRepeatedRow() throws DBAppException, ParseException {
        String strTableName = "StudentInt";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 0 );
        htblColNameValue.put("name", "Ahmed");
        htblColNameValue.put("gpa", 4.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
        Assertions.assertThrows(DBAppException.class, () -> {
                    DbApp.insertIntoTable(strTableName, htblColNameValue);
                }
        );
    }
    @Test
    @Order(14)
    void insertIntoTableDoubleRepeatedRow() throws DBAppException, ParseException {
        String strTableName = "StudentDouble";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 0 );
        htblColNameValue.put("name", "Ahmed");
        htblColNameValue.put("gpa", 4.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
        Assertions.assertThrows(DBAppException.class, () -> {
                    DbApp.insertIntoTable(strTableName, htblColNameValue);
                }
        );
    }
    @Test
    @Order(15)
    void insertIntoTableStringRepeatedRow() throws DBAppException, ParseException {
        String strTableName = "StudentString";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 0 );
        htblColNameValue.put("name", "Ahmed0");
        htblColNameValue.put("gpa", 4.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
        Assertions.assertThrows(DBAppException.class, () -> {
                    DbApp.insertIntoTable(strTableName, htblColNameValue);
                }
        );
    }
    @Test
    @Order(16)
    void insertIntoTableDateRepeatedRow() throws DBAppException, ParseException {
        String strTableName = "StudentDate";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 0 );
        htblColNameValue.put("name", "Ahmed");
        htblColNameValue.put("gpa", 4.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
        Assertions.assertThrows(DBAppException.class, () -> {
                    DbApp.insertIntoTable(strTableName, htblColNameValue);
                }
        );
    }
    @Test
    @Order(17)
    void insertIntoTableIntRowWithWrongData() throws DBAppException, ParseException {
        String strTableName = "StudentInt";

    }

    @Test
    @Order(20)
    void updateTable() {
    }

    @Test
    @Order(24)
    void deleteFromTable() {
    }
}