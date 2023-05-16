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
    static Hashtable<String, String> htblColNameType;
    static Hashtable<String, String> min;
    static Hashtable<String, String> max;

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
    // @Disabled()
    void createTableWithPrimaryKeyInt() throws DBAppException {
        String strTableName = "StudentInt";
        DbApp.createTable(strTableName, "id", htblColNameType, min, max);
    }

    @Test
    @Order(2)
    // @Disabled()
    void createTableWithPrimaryKeyDouble() throws DBAppException {
        String strTableName = "StudentDouble";
        DbApp.createTable(strTableName, "gpa", htblColNameType, min, max);
    }

    @Test
    @Order(3)
    // @Disabled()
    void createTableWithPrimaryKeyString() throws DBAppException {
        String strTableName = "StudentString";
        DbApp.createTable(strTableName, "name", htblColNameType, min, max);

    }

    @Test
    @Order(4)
    // @Disabled()
    void createTableWithPrimaryKeyDate() throws DBAppException {
        String strTableName = "StudentDate";
        DbApp.createTable(strTableName, "birthday", htblColNameType, min, max);

    }

    @Test
    @Order(5)
    void insertIntoTableIntNewRow() throws DBAppException, ParseException, IOException, ClassNotFoundException {
        String strTableName = "StudentInt";
        for (int i = 0; i < 5; i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", i);
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 4.0);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        }

        TablePersistence.printTable(strTableName);
    }

    @Test
    @Order(6)
    void insertIntoTableDoubleNewRow() throws DBAppException, ParseException {
        String strTableName = "StudentDouble";
        for (int i = 0; i <= 4; i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", i);
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", (double) i);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        }
    }

    @Test
    @Order(7)
    void insertIntoTableStringNewRow() throws DBAppException, ParseException {
        String strTableName = "StudentString";
        for (int i = 0; i < 5; i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", i);
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 4.0);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
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
            htblColNameValue.put("id", i);
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 4.0);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            htblColNameValue.put("birthday", formatter.parse("2000-01-0" + i));
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        }

    }

    @Test
    @Order(9)
    void checkTableLengthForTableInt() throws DBAppException {
        Assertions.assertEquals(5, DbApp.getTableLength("StudentInt"));
    }

    @Test
    @Order(10)
    void checkTableLengthForTableDouble() throws DBAppException {
        Assertions.assertEquals(5, DbApp.getTableLength("StudentDouble"));
    }

    @Test
    @Order(11)
    void checkTableLengthForTableString() throws  DBAppException {
        Assertions.assertEquals(5, DbApp.getTableLength("StudentString"));
    }

    @Test
    @Order(12)
    void checkTableLengthForTableDate() throws DBAppException {
        Assertions.assertEquals(9, DbApp.getTableLength("StudentDate"));
    }

    @Test
    @Order(13)
    void insertIntoTableIntRepeatedRow() throws DBAppException, ParseException {
        String strTableName = "StudentInt";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 0);
        htblColNameValue.put("name", "Ahmed");
        htblColNameValue.put("gpa", 4.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
        Assertions.assertThrows(DBAppException.class, () -> {
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        });
    }

    @Test
    @Order(14)
    void insertIntoTableDoubleRepeatedRow() throws DBAppException, ParseException {
        String strTableName = "StudentDouble";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 0);
        htblColNameValue.put("name", "Ahmed");
        htblColNameValue.put("gpa", 4.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
        Assertions.assertThrows(DBAppException.class, () -> {
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        });
    }

    @Test
    @Order(15)
    void insertIntoTableStringRepeatedRow() throws DBAppException, ParseException {
        String strTableName = "StudentString";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 0);
        htblColNameValue.put("name", "Ahmed0");
        htblColNameValue.put("gpa", 4.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
        Assertions.assertThrows(DBAppException.class, () -> {
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        });
    }

    @Test
    @Order(16)
    void insertIntoTableDateRepeatedRow() throws DBAppException, ParseException {
        String strTableName = "StudentDate";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 0);
        htblColNameValue.put("name", "Ahmed");
        htblColNameValue.put("gpa", 4.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
        Assertions.assertThrows(DBAppException.class, () -> {
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        });
    }

    @Test
    @Order(17)
    void insertIntoTableIntRowWithWrongData() throws DBAppException, ParseException {
        String strTableName = "StudentInt";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", "0"); // wrong data type
        htblColNameValue.put("name", "Amir");
        htblColNameValue.put("gpa", 4.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2000-01-02"));
        Assertions.assertThrows(DBAppException.class, () -> {
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        });

    }

    @Test
    @Order(18)
    void insertIntoTableStringRowWithWrongDate() throws DBAppException, ParseException {
        String strTableName = "StudentString";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 2);
        htblColNameValue.put("name", 114); // wrong data type
        htblColNameValue.put("gpa", 4.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2000-01-02"));
        Assertions.assertThrows(DBAppException.class, () -> {
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        });
    }

    @Test
    @Order(19)
    void insertIntoTableDateRowWithWrongDate() throws DBAppException, ParseException {
        String strTableName = "StudentDate";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 2);
        htblColNameValue.put("name", "Ahmed");
        htblColNameValue.put("gpa", 4.0);
        htblColNameValue.put("birthday", "1/2/1967"); // wrong data type
        Assertions.assertThrows(DBAppException.class, () -> {
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        });
    }

    @Test
    @Order(20)
    void updateTableInt() throws ParseException, DBAppException, IOException, ClassNotFoundException {
        String strTableName = "StudentInt";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        // htblColNameValue.put("id", 0);
        htblColNameValue.put("name", "Ahmed Wael");
        htblColNameValue.put("gpa", 0.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2020-01-01"));
        DbApp.updateTable(strTableName, "0", htblColNameValue);
        // System.out.println(DbApp.printTable(strTableName));
    }

    @Test
    @Order(21)
    void updateTableDouble() throws ParseException, DBAppException, IOException, ClassNotFoundException {
        String strTableName = "StudentDouble";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 0);
        htblColNameValue.put("name", "Ahmed Wael");
        // htblColNameValue.put("gpa", 0.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2020-01-01"));
        DbApp.updateTable(strTableName, "0.0", htblColNameValue);
        System.out.println(DbApp.printTable(strTableName));
    }

    @Test
    @Order(22)
    void updateTableString() throws ParseException, DBAppException, IOException, ClassNotFoundException {
        String strTableName = "StudentString";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 0);
        // htblColNameValue.put("name", "Ahmed Wael");
        htblColNameValue.put("gpa", 0.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2020-01-01"));
        DbApp.updateTable(strTableName, "Ahmed0", htblColNameValue);
        System.out.println(DbApp.printTable(strTableName));
    }

    @Test
    @Order(23)
    void updateTableDate() throws DBAppException, IOException, ClassNotFoundException, ParseException {
        String strTableName = "StudentDate";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 0);
        htblColNameValue.put("name", "Ahmed Wael");
        htblColNameValue.put("gpa", 0.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2020-01-01"));
        DbApp.updateTable(strTableName, "2000-01-01", htblColNameValue);
        // System.out.println(DbApp.printTable(strTableName));
    }

    @Test
    @Order(24)
    void deleteRowFromTable() throws DBAppException, ParseException {
        String strTableName = "StudentInt";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 1);
        htblColNameValue.put("name", "Ahmed1");
        htblColNameValue.put("gpa", 4.0);

        DbApp.deleteFromTable(strTableName, htblColNameValue);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2000-01-01"));

        System.out.println(htblColNameValue);
    }

    @Test
    @Order(25)
    void deleteNoneExistingRowFromTable() throws DBAppException, ParseException {
        String strTableName = "StudentInt";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 1);
        htblColNameValue.put("name", "Ahmed1");
        htblColNameValue.put("gpa", 4.0);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
        Assertions.assertThrows(DBAppException.class, () -> {
            DbApp.deleteFromTable(strTableName, htblColNameValue);
        });
    }

    @Test
    @Order(26)
    void createIndexForTheFirstTime() throws DBAppException, IOException, ParseException, ClassNotFoundException {
        String strTable = "StudentInt";
        Table table = DBApp.getTable(strTable);

        DbApp.createIndex(strTable, new String[] { "id", "name", "gpa" });
        System.out.println(table.getTableIndices().size());
        assertEquals(1, table.getTableIndices().entrySet().size());
    }


    void insertIntoTableIntAboveMax() throws ParseException {
        String strTableName = "StudentInt";
        for (int i = 0; i < 5; i++) {
            int random = (int) Math.random() * 9999;
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", 1000 + random);
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 4.0);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
            assertThrows(DBAppException.class, () -> {
                DbApp.insertIntoTable(strTableName, htblColNameValue);
            });
        }
    }

    @Test
    @Order(27)
    void insertIntoTableIntBelowMin() throws ParseException {
        String strTableName = "StudentInt";
        for (int i = 0; i < 5; i++) {
            int random = (int) Math.random() * 9999;
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", -1 * random);
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 4.0);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
            assertThrows(DBAppException.class, () -> {
                DbApp.insertIntoTable(strTableName, htblColNameValue);
            });
        }
    }

    @Test
    @Order(28)
    void insertIntoTableDoubleAboveMax() throws ParseException {
        String strTableName = "StudentDouble";
        for (int i = 0; i <= 4; i++) {
            int random = (int) Math.random() * 9999;
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", i);
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 4.0 + random);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
            assertThrows(DBAppException.class, () -> {
                DbApp.insertIntoTable(strTableName, htblColNameValue);
            });
        }
    }

    @Test
    @Order(28)
    void insertIntoTableDoubleBelowMin() throws ParseException {
        String strTableName = "StudentDouble";
        for (int i = 0; i <= 4; i++) {
            int random = (int) Math.random() * 9999;
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", i);
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 0.0 - random);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
            assertThrows(DBAppException.class, () -> {
                DbApp.insertIntoTable(strTableName, htblColNameValue);
            });
        }
    }

    @Test
    @Order(29)
    void insertIntoTableEmptyString() throws DBAppException, ParseException {
        String strTableName = "StudentString";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 6);
        htblColNameValue.put("name", "");
        htblColNameValue.put("gpa", 3.2);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
        assertThrows(DBAppException.class, () -> {
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        });
    }

    @Test
    @Order(30)
    void insertIntoTableStringAboveMax() throws DBAppException, ParseException {
        String strTableName = "StudentString";
        Hashtable<String, Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put("id", 6);
        htblColNameValue.put("name", "zzzzzzzzzzzzza");
        htblColNameValue.put("gpa", 3.2);
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        htblColNameValue.put("birthday", formatter.parse("2000-01-01"));
        assertThrows(DBAppException.class, () -> {
            DbApp.insertIntoTable(strTableName, htblColNameValue);
        });
    }

    @Test
    @Order(31)
    void insertIntoTableDateAboveMax() throws DBAppException, ParseException {
        String strTableName = "StudentDate";
        for (int i = 1; i <= 9; i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", i);
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 4.0);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            htblColNameValue.put("birthday", formatter.parse("2030-01-0" + i));
            assertThrows(DBAppException.class, () -> {
                DbApp.insertIntoTable(strTableName, htblColNameValue);
            });
        }

    }

    @Test
    @Order(32)
    void insertIntoTableDateBelowMin() throws DBAppException, ParseException {
        String strTableName = "StudentDate";
        for (int i = 1; i <= 9; i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", i);
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 4.0);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            htblColNameValue.put("birthday", formatter.parse("1030-01-0" + i));
            assertThrows(DBAppException.class, () -> {
                DbApp.insertIntoTable(strTableName, htblColNameValue);
            });
        }

    }

    @Test
    @Order(33)
    void insertIntoTableDateInvalidYear() throws DBAppException, ParseException {
        String strTableName = "StudentDate";
        for (int i = 3; i <= 9; i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", i);
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 4.0);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            htblColNameValue.put("birthday", formatter.parse(i + "020-01-01"));
            assertThrows(DBAppException.class, () -> {
                DbApp.insertIntoTable(strTableName, htblColNameValue);
            });
        }
    }

    @Test
    @Order(34)
    void insertIntoTableDateInvalidMonth() throws DBAppException, ParseException {
        String strTableName = "StudentDate";
        for (int i = 4; i <= 9; i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", i);
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 4.0);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            htblColNameValue.put("birthday", formatter.parse("220-" + i + "1-01"));
            assertThrows(DBAppException.class, () -> {
                DbApp.insertIntoTable(strTableName, htblColNameValue);
            });
        }
    }

    @Test
    @Order(35)
    void insertIntoTableDateInvalidDay() throws DBAppException, ParseException {
        String strTableName = "StudentDate";
        for (int i = 4; i <= 9; i++) {
            Hashtable<String, Object> htblColNameValue = new Hashtable<>();
            htblColNameValue.put("id", i);
            htblColNameValue.put("name", "Ahmed" + i);
            htblColNameValue.put("gpa", 4.0);
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            htblColNameValue.put("birthday", formatter.parse("220-01-" + i + "1"));
            assertThrows(DBAppException.class, () -> {
                DbApp.insertIntoTable(strTableName, htblColNameValue);
            });
        }
    }
}