package apoc.load.relative;

import apoc.load.LoadJson;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LoadJsonRelativePathTest {

    private GraphDatabaseService db;
    private static String PATH = new File(LoadCsvRelativePathTest.class.getClassLoader().getResource("map.json").getPath()).getParent();
	@Before public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("apoc.import.file.enabled","true")
                .setConfig("apoc.import.file.use_neo4j_config", "true")
                .setConfig("dbms.directories.import", PATH)
                .setConfig("dbms.security.allow_csv_import_from_file_urls","true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, LoadJson.class);
    }

    @After public void tearDown() {
	    db.shutdown();
    }

    @Test public void testLoadJson() {
	    String url = "map.json";
		testCall(db, "CALL apoc.load.json({url})",map("url",url), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("foo",asList(1,2,3)), row.get("value"));
                });
    }

    @Test public void testLoadMultiJson() {
        String url = "multi.json";
		testResult(db, "CALL apoc.load.json({url})",map("url",url), // 'file:map.json' YIELD value RETURN value
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("foo",asList(1,2,3)), row.get("value"));
                    row = result.next();
                    assertEquals(map("bar",asList(4,5,6)), row.get("value"));
                    assertFalse(result.hasNext());
                });
    }
    @Test public void testLoadMultiJsonPaths() {
        String url = "multi.json";
		testResult(db, "CALL apoc.load.json({url},'$')",map("url",url), // 'file:map.json' YIELD value RETURN value
                (result) -> {
                    Map<String, Object> row = result.next();
                    assertEquals(map("foo",asList(1,2,3)), row.get("value"));
                    row = result.next();
                    assertEquals(map("bar",asList(4,5,6)), row.get("value"));
                    assertFalse(result.hasNext());
                });
    }
    @Test public void testLoadJsonPath() {
        String url = "map.json";
		testCall(db, "CALL apoc.load.json({url},'$.foo')",map("url",url), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("result",asList(1,2,3)), row.get("value"));
                });
    }
    @Test public void testLoadJsonPathRoot() {
        String url = "map.json";
		testCall(db, "CALL apoc.load.json({url},'$')",map("url",url), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("foo",asList(1,2,3)), row.get("value"));
                });
    }
    @Test public void testLoadJsonArrayPath() {
        String url = "map.json";
		testCall(db, "CALL apoc.load.jsonArray({url},'$.foo')",map("url",url), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(asList(1,2,3), row.get("value"));
                });
    }
    @Test public void testLoadJsonArrayPathRoot() {
        String url = "map.json";
		testCall(db, "CALL apoc.load.jsonArray({url},'$')",map("url",url), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertEquals(map("foo",asList(1,2,3)), row.get("value"));
                });
    }

    @Test public void testLoadJsonNoFailOnError() {
        String url = "file.json";
        testResult(db, "CALL apoc.load.json({url},null, {failOnError:false})",map("url", url), // 'file:map.json' YIELD value RETURN value
                (row) -> {
                    assertFalse(row.hasNext());
                });
    }
}
