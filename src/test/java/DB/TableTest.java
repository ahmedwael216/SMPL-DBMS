package DB;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

class TableTest {
    @BeforeEach
    void setUp() {

    }

    @Test
    void testSetPageSize() throws IOException {
        String configFilePath = "src/main/java/DB/config/";
        if (Files.exists(Paths.get(configFilePath, "DBApp.config"))) {
            configFilePath = Paths.get(configFilePath, "DBApp.config").toString();
        } else if (Files.exists(Paths.get(configFilePath, "DBApp.properties"))) {
            configFilePath = Paths.get(configFilePath, "DBApp.properties").toString();
        } else {
            fail("No config file found");
        }

        boolean foundSizeOfPage = false;
        List<String> allLines = Files.readAllLines(Paths.get(configFilePath));
        for (int i = 0; i < allLines.size(); i++) {
            String line = allLines.get(i);
            if (line.toLowerCase().contains("maximumnumberofrows")) {
                foundSizeOfPage = true;
                break;
            }
        }

        if (!foundSizeOfPage) {
            fail("No page size found in config file");
        }

        Files.write(Paths.get(configFilePath), allLines);
    }

    @Test
    void testMetaDataFileExistanceForStudentTable() throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String tableName = "Student";
        Class cl = Class.forName(Table.class.getName());
        Hashtable<String, String> htblColNameType = new Hashtable<>(), htblColNameMin = new Hashtable<>(),
                htblColNameMax = new Hashtable<>();

        DbApp db = new DbApp();
        db.init();
        Constructor con = cl.getConstructor(String.class, String.class, Hashtable.class, Hashtable.class,
                Hashtable.class);
        Object obj = con.newInstance(tableName, "id", htblColNameType, htblColNameType, htblColNameType);
        String metaDataFilePath = DbApp.selectedDBName + "/" + tableName + "/Metadata.csv";
        if (!Files.exists(Paths.get(metaDataFilePath))) {
            fail("No metadata file found");
        }

        PrintWriter pw = new PrintWriter(metaDataFilePath);
        pw.close();
    }
}