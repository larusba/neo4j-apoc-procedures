package apoc.load.relative;

import apoc.load.LoadCsv;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.util.StringMap;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LoadCsvRelativePathTest {

    private GraphDatabaseService db;
    private static String PATH = new File(LoadCsvRelativePathTest.class.getClassLoader().getResource("test.csv").getPath()).getParent();

    @Before public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("apoc.import.file.enabled","true")
                .setConfig("apoc.import.file.use_neo4j_config", "true")
                .setConfig("dbms.directories.import", PATH)
                .setConfig("dbms.security.allow_csv_import_from_file_urls","true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, LoadCsv.class);
    }

    @After public void tearDown() {
        db.shutdown();
    }

    @Test public void testLoadCsv() throws Exception {
        String url = "test.csv";
        testResult(db, "CALL apoc.load.csv({url},{results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.csv'
                (r) -> {
                    assertRow(r,0L,"name","Selma","age","8");
                    assertRow(r,1L,"name","Rana","age","11");
                    assertRow(r,2L,"name","Selina","age","18");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvWithEmptyColumns() {
        String url = "empty_columns.csv";
        testResult(db, "CALL apoc.load.csv({url},{failOnError:false,mapping:{col_2:{type:'int'}}})", map("url",url), // 'file:test.csv'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("col_1", "1","col_2", null,"col_3", "1"), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", "2","col_2", 2L,"col_3", ""), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", "3","col_2", 3L,"col_3", "3"), row.get("map"));
                    assertEquals(false, r.hasNext());
                });
        testResult(db, "CALL apoc.load.csv({url},{failOnError:false,nullValues:[''], mapping:{col_1:{type:'int'}}})", map("url",url), // 'file:test.csv'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("col_1", 1L,"col_2", null,"col_3", "1"), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 2L,"col_2", "2","col_3", null), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", 3L,"col_2", "3","col_3", "3"), row.get("map"));
                    assertEquals(false, r.hasNext());
                });
        testResult(db, "CALL apoc.load.csv({url},{failOnError:false,mapping:{col_3:{type:'int',nullValues:['']}}})", map("url",url), // 'file:test.csv'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("col_1", "1","col_2", "","col_3", 1L), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", "2","col_2", "2","col_3", null), row.get("map"));
                    row = r.next();
                    assertEquals(map("col_1", "3","col_2", "3","col_3", 3L), row.get("map"));
                    assertEquals(false, r.hasNext());
                });
    }

    static void assertRow(Result r, long lineNo, Object...data) {
        Map<String, Object> row = r.next();
        Map<String, Object> map = map(data);
        List<Object> values = new ArrayList<>(map.values());
        StringMap stringMap = new StringMap();
        stringMap.putAll(map);
        assertEquals(map, row.get("map"));
        assertEquals(values, row.get("list"));
        assertEquals(values, row.get("strings"));
        assertEquals(stringMap, row.get("stringMap"));
        assertEquals(lineNo, row.get("lineNo"));
    }

    @Test public void testLoadCsvSkip() {
        String url = "test.csv";
        testResult(db, "CALL apoc.load.csv({url},{skip:1,limit:1,results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.csv'
                (r) -> {
                    assertRow(r,1L,"name","Rana", "age", "11");
                    assertEquals(false, r.hasNext());
                });
    }
    @Test public void testLoadCsvTabSeparator() {
        String url = "test-tab.csv";
        testResult(db, "CALL apoc.load.csv({url},{sep:'TAB',results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.csv'
                (r) -> {
                    assertRow(r, 0L,"name", "Rana", "age","11");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvNoHeader() {
        String url = "test-no-header.csv";
        testResult(db, "CALL apoc.load.csv({url},{header:false,results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.csv'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(null, row.get("map"));
                    assertEquals(asList("Selma", "8"), row.get("list"));
                    assertEquals(0L, row.get("lineNo"));
                    assertEquals(false, r.hasNext());
                });
    }
    @Test public void testLoadCsvIgnoreFields() {
        String url = "test-tab.csv";
        testResult(db, "CALL apoc.load.csv({url},{ignore:['age'],sep:'TAB',results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.csv'
                (r) -> {
                    assertRow(r,0L,"name","Rana");
                    assertEquals(false, r.hasNext());
                });
    }

    @Test public void testLoadCsvColonSeparator() {
        String url = "test.dsv";
        testResult(db, "CALL apoc.load.csv({url},{sep:':',results:['map','list','stringMap','strings']})", map("url",url), // 'file:test.dsv'
                (r) -> {
                    assertRow(r,0L,"name","Rana","age","11");
                    assertFalse(r.hasNext());
                });
    }

    @Test public void testPipeArraySeparator() {
        String url = "test-pipe-column.csv";
        testResult(db, "CALL apoc.load.csv({url},{results:['map','list','stringMap','strings'],mapping:{name:{type:'string'},beverage:{array:true,arraySep:'|',type:'string'}}})", map("url",url.toString()), // 'file:test.csv'
                (r) -> {
                    assertEquals(asList("Selma", asList("Soda")), r.next().get("list"));
                    assertEquals(asList("Rana", asList("Tea", "Milk")), r.next().get("list"));
                    assertEquals(asList("Selina", asList("Cola")), r.next().get("list"));
                });
    }

    @Test public void testMapping() {
        String url = "test-mapping.csv";
        testResult(db, "CALL apoc.load.csv({url},{results:['map','list','stringMap','strings'],mapping:{name:{type:'string'},age:{type:'int'},kids:{array:true,arraySep:':',type:'int'},pass:{ignore:true}}})", map("url",url.toString()), // 'file:test.csv'
                (r) -> {
                    Map<String, Object> row = r.next();
                    assertEquals(map("name", "Michael", "age", 41L, "kids", asList(8L, 11L, 18L)), row.get("map"));
                    assertEquals(map("name", "Michael", "age", "41", "kids", "8:11:18"), row.get("stringMap"));
                    assertEquals(asList("Michael", 41L, asList(8L, 11L, 18L)), row.get("list"));
                    assertEquals(asList("Michael", "41", "8:11:18"), row.get("strings"));
                    assertEquals(0L, row.get("lineNo"));
                    assertEquals(false, r.hasNext());
                });
    }

}