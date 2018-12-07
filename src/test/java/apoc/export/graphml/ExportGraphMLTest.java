package apoc.export.graphml;

import apoc.graph.Graphs;
import apoc.util.TestUtil;
import junit.framework.TestCase;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.FileWriter;
import java.util.Map;
import java.util.Scanner;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author mh
 * @since 22.05.16
 */
public class ExportGraphMLTest {

    public static final String HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>%n" +
            "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd\">%n";
    public static final String KEY_TYPES_FALSE = "<key id=\"born\" for=\"node\" attr.name=\"born\"/>%n" +
            "<key id=\"values\" for=\"node\" attr.name=\"values\"/>%n" +
            "<key id=\"name\" for=\"node\" attr.name=\"name\"/>%n" +
            "<key id=\"label\" for=\"node\" attr.name=\"label\"/>%n"+
            "<key id=\"place\" for=\"node\" attr.name=\"place\"/>%n" +
            "<key id=\"TYPE\" for=\"node\" attr.name=\"TYPE\"/>%n" +
            "<key id=\"age\" for=\"node\" attr.name=\"age\"/>%n" +
            "<key id=\"label\" for=\"edge\" attr.name=\"label\"/>%n" +
            "<key id=\"TYPE\" for=\"edge\" attr.name=\"TYPE\"/>%n";
    public static final String KEY_TYPES = "<key id=\"born\" for=\"node\" attr.name=\"born\" attr.type=\"string\"/>%n" +
            "<key id=\"values\" for=\"node\" attr.name=\"values\" attr.type=\"string\" attr.list=\"long\"/>%n" +
            "<key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>%n" +
            "<key id=\"label\" for=\"node\" attr.name=\"label\" attr.type=\"string\"/>%n"+
            "<key id=\"place\" for=\"node\" attr.name=\"place\" attr.type=\"string\"/>%n" +
            "<key id=\"TYPE\" for=\"node\" attr.name=\"TYPE\" attr.type=\"string\"/>%n" +
            "<key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"long\"/>%n" +
            "<key id=\"label\" for=\"edge\" attr.name=\"label\" attr.type=\"string\"/>%n" +
            "<key id=\"TYPE\" for=\"edge\" attr.name=\"TYPE\" attr.type=\"string\"/>%n";
    public static final String KEY_TYPES_PATH = "<key id=\"born\" for=\"node\" attr.name=\"born\" attr.type=\"string\"/>%n" +
            "<key id=\"name\" for=\"node\" attr.name=\"name\" attr.type=\"string\"/>%n" +
            "<key id=\"label\" for=\"node\" attr.name=\"label\" attr.type=\"string\"/>%n"+
            "<key id=\"place\" for=\"node\" attr.name=\"place\" attr.type=\"string\"/>%n" +
            "<key id=\"TYPE\" for=\"node\" attr.name=\"TYPE\" attr.type=\"string\"/>%n" +
            "<key id=\"age\" for=\"node\" attr.name=\"age\" attr.type=\"long\"/>%n" +
            "<key id=\"label\" for=\"edge\" attr.name=\"label\" attr.type=\"string\"/>%n" +
            "<key id=\"TYPE\" for=\"edge\" attr.name=\"TYPE\" attr.type=\"string\"/>%n";
    public static final String GRAPH = "<graph id=\"G\" edgedefault=\"directed\">%n";
    public static final String DATA = "<node id=\"n0\" labels=\":Foo:Foo0:Foo2\"><data key=\"TYPE\">:Foo:Foo0:Foo2</data><data key=\"labels\">:Foo:Foo0:Foo2</data><data key=\"place\">{\"crs\":\"wgs-84-3d\",\"latitude\":56.7,\"longitude\":12.78,\"height\":100.0}</data><data key=\"name\">foo</data><data key=\"born\">2018-10-10</data></node>%n" +
            "<node id=\"n1\" labels=\":Bar\"><data key=\"TYPE\">:Bar</data><data key=\"labels\">:Bar</data><data key=\"age\">42</data><data key=\"name\">bar</data><data key=\"place\">{\"crs\":\"wgs-84\",\"latitude\":56.7,\"longitude\":12.78,\"height\":null}</data></node>%n" +
            "<node id=\"n2\" labels=\":Bar\"><data key=\"TYPE\">:Bar</data><data key=\"labels\">:Bar</data><data key=\"age\">12</data><data key=\"values\">[1,2,3]</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\" label=\"KNOWS\"><data key=\"label\">KNOWS</data><data key=\"TYPE\">KNOWS</data></edge>%n";
    public static final String DATA_PATH = "<node id=\"n0\" labels=\":Foo:Foo0:Foo2\"><data key=\"TYPE\">:Foo:Foo0:Foo2</data><data key=\"label\">:Foo:Foo0:Foo2</data><data key=\"place\">{\"crs\":\"wgs-84-3d\",\"latitude\":56.7,\"longitude\":12.78,\"height\":100.0}</data><data key=\"name\">foo</data><data key=\"born\">2018-10-10</data></node>%n" +
            "<node id=\"n1\" labels=\":Bar\"><data key=\"TYPE\">:Bar</data><data key=\"label\">:Bar</data><data key=\"age\">42</data><data key=\"name\">bar</data><data key=\"place\">{\"crs\":\"wgs-84\",\"latitude\":56.7,\"longitude\":12.78,\"height\":null}</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\" label=\"KNOWS\"><data key=\"label\">KNOWS</data><data key=\"TYPE\">KNOWS</data></edge>%n";
    public static final String DATA_PATH_CAPTION = "<node id=\"n0\" labels=\"foo\"><data key=\"TYPE\">:Foo:Foo0:Foo2</data><data key=\"label\">foo</data><data key=\"place\">{\"crs\":\"wgs-84-3d\",\"latitude\":56.7,\"longitude\":12.78,\"height\":100.0}</data><data key=\"name\">foo</data><data key=\"born\">2018-10-10</data></node>%n" +
            "<node id=\"n1\" labels=\"bar\"><data key=\"TYPE\">:Bar</data><data key=\"label\">bar</data><data key=\"age\">42</data><data key=\"name\">bar</data><data key=\"place\">{\"crs\":\"wgs-84\",\"latitude\":56.7,\"longitude\":12.78,\"height\":null}</data></node>%n" +
            "<edge id=\"e0\" source=\"n0\" target=\"n1\" label=\"KNOWS\"><data key=\"label\">KNOWS</data><data key=\"TYPE\">KNOWS</data></edge>%n";
    public static final String FOOTER = "</graph>%n" +
            "</graphml>";
    private static final String EXPECTED = String.format(HEADER + KEY_TYPES_FALSE + GRAPH + DATA + FOOTER);
    private static final String EXPECTED_TYPES = String.format(HEADER + KEY_TYPES + GRAPH + DATA + FOOTER);
    private static final String EXPECTED_TYPES_PATH = String.format(HEADER + KEY_TYPES_PATH + GRAPH + DATA_PATH + FOOTER);
    private static final String EXPECTED_TYPES_PATH_CAPTION = String.format(HEADER + KEY_TYPES_PATH + GRAPH + DATA_PATH_CAPTION + FOOTER);

    private static GraphDatabaseService db;
    private static File directory = new File("target/import");

    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }

    @Rule
    public TestName testName = new TestName();

    private static final String TEST_WITH_NO_IMPORT = "WithNoImportConfig";
    private static final String TEST_WITH_NO_EXPORT = "WithNoExportConfig";

    @Before
    public void setUp() throws Exception {
        GraphDatabaseBuilder builder  = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(GraphDatabaseSettings.load_csv_file_url_root, directory.getAbsolutePath());
        if (!testName.getMethodName().endsWith(TEST_WITH_NO_EXPORT)) {
            builder.setConfig("apoc.export.file.enabled", "true");
        }
        if (!testName.getMethodName().endsWith(TEST_WITH_NO_IMPORT)) {
            builder.setConfig("apoc.import.file.enabled", "true");
        }
        db = builder.newGraphDatabase();
        TestUtil.registerProcedure(db, ExportGraphML.class, Graphs.class);
        db.execute("CREATE (f:Foo:Foo2:Foo0 {name:'foo', born:Date('2018-10-10'), place:point({ longitude: 56.7, latitude: 12.78, height: 100 })})-[:KNOWS]->(b:Bar {name:'bar',age:42, place:point({ longitude: 56.7, latitude: 12.78})}),(c:Bar {age:12,values:[1,2,3]})").close();
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testImportGraphML() throws Exception {
        db.execute("MATCH (n) DETACH DELETE n").close();

        File output = new File(directory, "import.graphml");
        FileWriter fw = new FileWriter(output);
        fw.write(EXPECTED_TYPES); fw.close();
        TestUtil.testCall(db, "CALL apoc.import.graphml({file},{readLabels:true})", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertEquals(3L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(12L, r.get("properties"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });

        TestUtil.testCall(db, "MATCH  (c:Bar {age: 12, values: [1,2,3]}) RETURN COUNT(c) AS c", null, (r) -> assertEquals(1L, r.get("c")));
    }

    @Test(expected = QueryExecutionException.class)
    public void testImportGraphMLWithNoImportConfig() throws Exception {
        File output = new File(directory, "all.graphml");
        try {
            TestUtil.testCall(db, "CALL apoc.import.graphml({file},{readLabels:true})", map("file", output.getAbsolutePath()),(r) -> assertResults(output, r, "database"));
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            TestCase.assertTrue(except instanceof RuntimeException);
            assertEquals("Import from files not enabled, please set apoc.import.file.enabled=true in your neo4j.conf", except.getMessage());
            throw e;
        }
    }

    @Test
    public void testExportAllGraphML() throws Exception {
        File output = new File(directory, "all.graphml");
        TestUtil.testCall(db, "CALL apoc.export.graphml.all({file},null)", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "database"));
        assertEquals(EXPECTED, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportGraphGraphML() throws Exception {
        File output = new File(directory, "graph.graphml");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.graphml.graph(graph, {file},null) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED, new Scanner(output).useDelimiter("\\Z").next());
    }
    @Test
    public void testExportGraphGraphMLTypes() throws Exception {
        File output = new File(directory, "graph.graphml");
        TestUtil.testCall(db, "CALL apoc.graph.fromDB('test',{}) yield graph " +
                        "CALL apoc.export.graphml.graph(graph, {file},{useTypes:true}) " +
                        "YIELD nodes, relationships, properties, file, source,format, time " +
                        "RETURN *", map("file", output.getAbsolutePath()),
                (r) -> assertResults(output, r, "graph"));
        assertEquals(EXPECTED_TYPES, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test(expected = QueryExecutionException.class)
    public void testExportGraphGraphMLTypesWithNoExportConfig() throws Exception {
        File output = new File(directory, "all.graphml");
        try {
            TestUtil.testCall(db, "CALL apoc.export.graphml.all({file},null)", map("file", output.getAbsolutePath()), (r) -> assertResults(output, r, "database"));
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            TestCase.assertTrue(except instanceof RuntimeException);
            assertEquals("Export to files not enabled, please set apoc.export.file.enabled=true in your neo4j.conf", except.getMessage());
            throw e;
        }
    }

    @Test
    public void testExportGraphGraphMLQueryGephi() throws Exception {
        File output = new File(directory, "query.graphml");
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',{file},{useTypes:true, format: 'gephi'}) ", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(6L, r.get("properties"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });
        assertEquals(EXPECTED_TYPES_PATH, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportGraphGraphMLQueryGephiWithArrayCaption() throws Exception {
        File output = new File(directory, "query.graphml");
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',{file},{useTypes:true, format: 'gephi', caption: ['bar','name','foo']}) ", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(6L, r.get("properties"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });
        assertEquals(EXPECTED_TYPES_PATH_CAPTION, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test
    public void testExportGraphGraphMLQueryGephiWithArrayCaptionWrong() throws Exception {
        File output = new File(directory, "query.graphml");
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',{file},{useTypes:true, format: 'gephi', caption: ['a','b','c']}) ", map("file", output.getAbsolutePath()),
                (r) -> {
                    assertEquals(2L, r.get("nodes"));
                    assertEquals(1L, r.get("relationships"));
                    assertEquals(6L, r.get("properties"));
                    assertEquals(output.getAbsolutePath(), r.get("file"));
                    if (r.get("source").toString().contains(":"))
                        assertEquals("statement" + ": nodes(2), rels(1)", r.get("source"));
                    else
                        assertEquals("file", r.get("source"));
                    assertEquals("graphml", r.get("format"));
                    assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
                });
        assertEquals(EXPECTED_TYPES_PATH, new Scanner(output).useDelimiter("\\Z").next());
    }

    @Test(expected = QueryExecutionException.class)
    public void testExportGraphGraphMLQueryGephiWithStringCaption() throws Exception {
        File output = new File(directory, "query.graphml");
        try {
        TestUtil.testCall(db, "call apoc.export.graphml.query('MATCH p=()-[r]->() RETURN p limit 1000',{file},{useTypes:true, format: 'gephi', caption: 'name'}) ", map("file", output.getAbsolutePath()),
                (r) -> {});
        } catch (QueryExecutionException e) {
            Throwable except = ExceptionUtils.getRootCause(e);
            TestCase.assertTrue(except instanceof RuntimeException);
            assertEquals("java.lang.String cannot be cast to java.util.List", except.getMessage());
            throw e;
        }
    }

    private void assertResults(File output, Map<String, Object> r, final String source) {
        assertEquals(3L, r.get("nodes"));
        assertEquals(1L, r.get("relationships"));
        assertEquals(8L, r.get("properties"));
        assertEquals(output.getAbsolutePath(), r.get("file"));
        if (r.get("source").toString().contains(":"))
            assertEquals(source + ": nodes(3), rels(1)", r.get("source"));
        else
            assertEquals("file", r.get("source"));
        assertEquals("graphml", r.get("format"));
        assertTrue("Should get time greater than 0",((long) r.get("time")) > 0);
    }
}
