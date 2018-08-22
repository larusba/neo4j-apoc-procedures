package apoc.load.relative;

import apoc.load.Xml;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static apoc.load.relative.LoadXmlResult.*;
import static apoc.util.TestUtil.*;
import static org.junit.Assert.*;

public class LoadXmlRelativePathTest {

    private GraphDatabaseService db;
    private static String PATH = new File(LoadCsvRelativePathTest.class.getClassLoader().getResource("xml/mixedcontent.xml").getPath()).getParent();

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig("dbms.directories.import", PATH )
                .setConfig("d0bms.security.allow_csv_import_from_file_urls","true")
                .setConfig("apoc.import.file.enabled","true")
                .setConfig("apoc.import.file.use_neo4j_config", "true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, Xml.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testLoadXml() {
        testCall(db, "CALL apoc.load.xml('databases.xml')", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    assertEquals(StringXmlNestedSimpleMap(), value.toString());
                });
    }

    @Test
    public void testLoadXmlSimple() {
        testCall(db, "CALL apoc.load.xmlSimple('databases.xml')", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    assertEquals(StringXmlNestedMap(), value.toString());
                });
    }

    @Test
    public void testMixedContent() {
        testCall(db, "CALL apoc.load.xml('mixedcontent.xml')", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    assertEquals("{_type=root, _children=[{_type=text, _children=[{_type=mixed}, text0, text1]}, {_type=text, _text=text as cdata}]}", value.toString());

                });
    }

    @Test
    public void testBookIds() {
        testResult(db, "call apoc.load.xml('books.xml') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test
    public void testFilterIntoCollection() {
        File file = new File(PATH + "/bookResult.txt");
        testResult(db, "call apoc.load.xml('books.xml') yield value as catalog\n" +
                        "    UNWIND catalog._children as book\n" +
                        "    RETURN book.id, [attr IN book._children WHERE attr._type IN ['author','title'] | [attr._type, attr._text]] as pairs"
                , result -> {
                    try (Scanner scanner = new Scanner(file).useDelimiter("\\Z")) {
                        assertEquals(scanner.next(), result.resultAsString());
                    } catch (FileNotFoundException e) {}
                });
    }

    @Test
    public void testReturnCollectionElements() {
        File file = new File(PATH + "/bookResult.txt");
        testResult(db, "call apoc.load.xml('books.xml') yield value as catalog\n"+
                        "UNWIND catalog._children as book\n" +
                        "WITH book.id as id, [attr IN book._children WHERE attr._type IN ['author','title'] | attr._text] as pairs\n" +
                        "RETURN id, pairs[0] as author, pairs[1] as title"
                , result -> {
                    try (Scanner scanner = new Scanner(file).useDelimiter("\\Z")) {
                        assertEquals(scanner.next(), result.resultAsString());
                    } catch (FileNotFoundException e) {}
                });
    }

    @Test
    public void testLoadXmlXpathAuthorFromBookId () {
        testCall(db, "CALL apoc.load.xml('books.xml', '/catalog/book[@id=\"bk102\"]/author') yield value as result",
                (r) -> {
                    assertEquals("author", ((Map) r.get("result")).get("_type"));
                    assertEquals("Ralls, Kim", ((Map) r.get("result")).get("_text"));
                });
    }

    @Test
    public void testLoadXmlXpathGenreFromBookTitle () {
        testCall(db, "CALL apoc.load.xml('books.xml', '/catalog/book[title=\"Maeve Ascendant\"]/genre') yield value as result",
                (r) -> {
                    assertEquals("genre", ((Map) r.get("result")).get("_type"));
                    assertEquals("Fantasy", ((Map) r.get("result")).get("_text"));
                });
    }

    @Test
    public void testLoadXmlXpathReturnBookFromBookTitle () {
        testCall(db, "CALL apoc.load.xml('books.xml', '/catalog/book[title=\"Maeve Ascendant\"]/.') yield value as result",
                (r) -> {
                    Object value = r.values();
                    assertEquals(StringXmlXPathNestedMap(), value.toString());
                });
    }

    @Test
    public void testLoadXmlXpathBooKsFromGenre () {
            testResult(db, "CALL apoc.load.xml('books.xml', '/catalog/book[genre=\"Computer\"]') yield value as result",
                (r) -> {
                    Map<String, Object> result = (Map<String, Object>) r.next().get("result");

                    Object children = result.get("_children");

                    List<Object>  childrenList = (List<Object>) children;
                    Map<String, String> childrenMap = ((Map<String, String>) childrenList.get(0));

                    assertEquals("bk101", result.get("id"));
                    assertEquals("author", childrenMap.get("_type"));
                    assertEquals("Gambardella, Matthew", childrenMap.get("_text"));
                    assertEquals("author", ((Map) childrenList.get(1)).get("_type"));
                    assertEquals("Arciniegas, Fabio", ((Map) childrenList.get(1)).get("_text"));

                    result = (Map<String, Object>) r.next().get("result");
                    children = result.get("_children");
                    childrenList = (List<Object>) children;
                    childrenMap = ((Map<String, String>) childrenList.get(0));

                    assertEquals("bk110", result.get("id"));
                    assertEquals("author", childrenMap.get("_type"));
                    assertEquals("O'Brien, Tim", childrenMap.get("_text"));
                    assertEquals("title", ((Map) childrenList.get(1)).get("_type"));
                    assertEquals("Microsoft .NET: The Programming Bible", ((Map) childrenList.get(1)).get("_text"));

                    result = (Map<String, Object>) r.next().get("result");
                    children = result.get("_children");
                    childrenList = (List<Object>) children;
                    childrenMap = ((Map<String, String>) childrenList.get(0));

                    assertEquals("bk111", result.get("id"));
                    assertEquals("author", childrenMap.get("_type"));
                    assertEquals("O'Brien, Tim", childrenMap.get("_text"));
                    assertEquals("title", ((Map) childrenList.get(1)).get("_type"));
                    assertEquals("MSXML3: A Comprehensive Guide", ((Map) childrenList.get(1)).get("_text"));

                    result = (Map<String, Object>) r.next().get("result");
                    children = result.get("_children");
                    childrenList = (List<Object>) children;
                    childrenMap = ((Map<String, String>) childrenList.get(0));

                    assertEquals("bk112", result.get("id"));
                    assertEquals("author", childrenMap.get("_type"));
                    assertEquals("Galos, Mike", childrenMap.get("_text"));
                    assertEquals("title", ((Map) childrenList.get(1)).get("_type"));
                    assertEquals("Visual Studio 7: A Comprehensive Guide", ((Map) childrenList.get(1)).get("_text"));
                    assertEquals(false, r.hasNext());
                });
    }

    @Test
    public void testLoadXmlNoFailOnError() {
        testCall(db, "CALL apoc.load.xml('books.xm', '', {failOnError:false}) yield value as result",
                (r) -> {
                    Map resultMap = (Map) r.get("result");
                    assertEquals(Collections.emptyMap(), resultMap);
                });
    }

    @Test
    public void testLoadXmlWithImport() {
        testCall(db, "call apoc.xml.import('humboldt_soemmering01_1791.TEI-P5.xml', {createNextWordRelationships: true}) yield node",
                row -> {
                   assertNotNull(row.get("node"));
                });
        testResult(db, "match (n) return labels(n)[0] as label, count(*) as count", result -> {
            final Map<String, Long> resultMap = result.stream().collect(Collectors.toMap(o -> o.get("label").toString(), o -> (Long)o.get("count")));
            assertEquals(2l,resultMap.get("XmlProcessingInstruction").longValue());
            assertEquals(1l, resultMap.get("XmlDocument").longValue());
            assertEquals(1737l, resultMap.get("XmlWord").longValue());
            assertEquals(454l, resultMap.get("XmlTag").longValue());
        });

        // no node more than one NEXT/NEXT_SIBLING
        testCallEmpty(db, "match (n) where size( (n)-[:NEXT]->() ) > 1 return n", null);
        testCallEmpty(db, "match (n) where size( (n)-[:NEXT_SIBLING]->() ) > 1 return n", null);

        // no node more than one IS_FIRST_CHILD / IS_LAST_CHILD
        testCallEmpty(db, "match (n) where size( (n)<-[:FIRST_CHILD_OF]-() ) > 1 return n", null);
        testCallEmpty(db, "match (n) where size( (n)<-[:LAST_CHILD_OF]-() ) > 1 return n", null);

        // NEXT_WORD relationship do connect all word nodes
        testResult(db, "match p=(:XmlDocument)-[:NEXT_WORD*]->(e:XmlWord) where not (e)-[:NEXT_WORD]->() return length(p) as len",
                result -> {
                    Map<String, Object> r = Iterators.single(result);
                    assertEquals(1737l, r.get("len"));
                });

    }
}
