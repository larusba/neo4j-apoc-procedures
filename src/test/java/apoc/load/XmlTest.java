package apoc.load;

import apoc.util.TestUtil;
import org.apache.commons.lang.math.IntRange;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.net.ConnectException;
import java.sql.Time;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.*;
import java.util.stream.IntStream;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class XmlTest {

    public static final String XML_AS_NESTED_MAP =
            "{_type=parent, name=databases, " +
                    "_children=[" +
                    "{_type=child, name=Neo4j, _text=Neo4j is a graph database}, " +
                    "{_type=child, name=relational, _children=[" +
                    "{_type=grandchild, name=MySQL, _text=MySQL is a database & relational}, " +
                    "{_type=grandchild, name=Postgres, _text=Postgres is a relational database}]}]}";
    public static final String XML_AS_NESTED_SIMPLE_MAP =
            "{_type=parent, name=databases, " +
                    "_child=[" +
                    "{_type=child, name=Neo4j, _text=Neo4j is a graph database}, " +
                    "{_type=child, name=relational, _grandchild=[" +
                    "{_type=grandchild, name=MySQL, _text=MySQL is a database & relational}, " +
                    "{_type=grandchild, name=Postgres, _text=Postgres is a relational database}]}]}";
    public static final String XML_XPATH_AS_NESTED_MAP =
            "[{_type=book, id=bk103, _children=[{_type=author, _text=Corets, Eva}, {_type=title, _text=Maeve Ascendant}, {_type=genre, _text=Fantasy}, {_type=price, _text=5.95}, {_type=publish_date, _text=2000-11-17}, {_type=description, _text=After the collapse of a nanotechnology " +
                    "society in England, the young survivors lay the " +
                    "foundation for a new society.}]}]";
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig("apoc.import.file.enabled", "true").newGraphDatabase();
        TestUtil.registerProcedure(db, Xml.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testLoadXml() throws Exception {
        testCall(db, "CALL apoc.load.xml('file:databases.xml')", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    assertEquals(XML_AS_NESTED_MAP, value.toString());
                });
    }

    @Test
    public void testLoadXmlSimple() throws Exception {
        testCall(db, "CALL apoc.load.xmlSimple('file:databases.xml')", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    assertEquals(XML_AS_NESTED_SIMPLE_MAP, value.toString());
                });
    }

    @Test
    public void testMixedContent() {
        testCall(db, "CALL apoc.load.xml('file:src/test/resources/mixedcontent.xml')", //  YIELD value RETURN value
                (row) -> {
                    Object value = row.get("value");
                    //assertEquals("{_type=root, _children=[{_type=text, _children=[text0, {_type=mixed}, text1]}, {_type=text, _text=text as cdata}]}", value.toString());
                    assertEquals("{_type=root, _children=[{_type=text, _children=[{_type=mixed}, text0, text1]}, {_type=text, _text=text as cdata}]}", value.toString());

                });
    }

    @Test
    public void testBookIds() {
        testResult(db, "call apoc.load.xml('file:src/test/resources/books.xml') yield value as catalog\n" +
                "UNWIND catalog._children as book\n" +
                "RETURN book.id as id\n", result -> {
            List<Object> ids = Iterators.asList(result.columnAs("id"));
            assertTrue(IntStream.rangeClosed(1,12).allMatch(value -> ids.contains(String.format("bk1%02d",value))));
        });
    }

    @Test
    public void testFilterIntoCollection() {
        testResult(db, "call apoc.load.xml('file:src/test/resources/books.xml') yield value as catalog\n" +
                        "    UNWIND catalog._children as book\n" +
                        "    RETURN book.id, [attr IN book._children WHERE attr._type IN ['author','title'] | [attr._type, attr._text]] as pairs"
                , result -> {
                    assertEquals("+----------------------------------------------------------------------------------------------------------------+\n" +
                            "| book.id | pairs                                                                                                |\n" +
                            "+----------------------------------------------------------------------------------------------------------------+\n" +
                            "| \"bk101\" | [[\"author\",\"Gambardella, Matthew\"],[\"author\",\"Arciniegas, Fabio\"],[\"title\",\"XML Developer's Guide\"]] |\n" +
                            "| \"bk102\" | [[\"author\",\"Ralls, Kim\"],[\"title\",\"Midnight Rain\"]]                                                  |\n" +
                            "| \"bk103\" | [[\"author\",\"Corets, Eva\"],[\"title\",\"Maeve Ascendant\"]]                                               |\n" +
                            "| \"bk104\" | [[\"author\",\"Corets, Eva\"],[\"title\",\"Oberon's Legacy\"]]                                               |\n" +
                            "| \"bk105\" | [[\"author\",\"Corets, Eva\"],[\"title\",\"The Sundered Grail\"]]                                            |\n" +
                            "| \"bk106\" | [[\"author\",\"Randall, Cynthia\"],[\"title\",\"Lover Birds\"]]                                              |\n" +
                            "| \"bk107\" | [[\"author\",\"Thurman, Paula\"],[\"title\",\"Splish Splash\"]]                                              |\n" +
                            "| \"bk108\" | [[\"author\",\"Knorr, Stefan\"],[\"title\",\"Creepy Crawlies\"]]                                             |\n" +
                            "| \"bk109\" | [[\"author\",\"Kress, Peter\"],[\"title\",\"Paradox Lost\"]]                                                 |\n" +
                            "| \"bk110\" | [[\"author\",\"O'Brien, Tim\"],[\"title\",\"Microsoft .NET: The Programming Bible\"]]                        |\n" +
                            "| \"bk111\" | [[\"author\",\"O'Brien, Tim\"],[\"title\",\"MSXML3: A Comprehensive Guide\"]]                                |\n" +
                            "| \"bk112\" | [[\"author\",\"Galos, Mike\"],[\"title\",\"Visual Studio 7: A Comprehensive Guide\"]]                        |\n" +
                            "+----------------------------------------------------------------------------------------------------------------+\n" +
                            "12 rows\n", result.resultAsString());
                });
    }

    @Test
    public void testReturnCollectionElements() {
        testResult(db, "call apoc.load.xml('file:src/test/resources/books.xml') yield value as catalog\n"+
                        "UNWIND catalog._children as book\n" +
                        "WITH book.id as id, [attr IN book._children WHERE attr._type IN ['author','title'] | attr._text] as pairs\n" +
                        "RETURN id, pairs[0] as author, pairs[1] as title"
                , result -> {
                    assertEquals("+-----------------------------------------------------------------------------+\n" +
                            "| id      | author                 | title                                    |\n" +
                            "+-----------------------------------------------------------------------------+\n" +
                            "| \"bk101\" | \"Gambardella, Matthew\" | \"Arciniegas, Fabio\"                      |\n" +
                            "| \"bk102\" | \"Ralls, Kim\"           | \"Midnight Rain\"                          |\n" +
                            "| \"bk103\" | \"Corets, Eva\"          | \"Maeve Ascendant\"                        |\n" +
                            "| \"bk104\" | \"Corets, Eva\"          | \"Oberon's Legacy\"                        |\n" +
                            "| \"bk105\" | \"Corets, Eva\"          | \"The Sundered Grail\"                     |\n" +
                            "| \"bk106\" | \"Randall, Cynthia\"     | \"Lover Birds\"                            |\n" +
                            "| \"bk107\" | \"Thurman, Paula\"       | \"Splish Splash\"                          |\n" +
                            "| \"bk108\" | \"Knorr, Stefan\"        | \"Creepy Crawlies\"                        |\n" +
                            "| \"bk109\" | \"Kress, Peter\"         | \"Paradox Lost\"                           |\n" +
                            "| \"bk110\" | \"O'Brien, Tim\"         | \"Microsoft .NET: The Programming Bible\"  |\n" +
                            "| \"bk111\" | \"O'Brien, Tim\"         | \"MSXML3: A Comprehensive Guide\"          |\n" +
                            "| \"bk112\" | \"Galos, Mike\"          | \"Visual Studio 7: A Comprehensive Guide\" |\n" +
                            "+-----------------------------------------------------------------------------+\n" +
                            "12 rows\n", result.resultAsString());
                });
    }

    @Test
    public void testLoadXmlXpathAuthorFromBookId () {
        testCall(db, "CALL apoc.load.xml('file:src/test/resources/books.xml', '/catalog/book[@id=\"bk102\"]/author') yield value as result",
                (r) -> {
                    assertEquals("author", ((Map) r.get("result")).get("_type"));
                    assertEquals("Ralls, Kim", ((Map) r.get("result")).get("_text"));
                });
    }

    @Test
    public void testLoadXmlXpathGenreFromBookTitle () {
        testCall(db, "CALL apoc.load.xml('file:src/test/resources/books.xml', '/catalog/book[title=\"Maeve Ascendant\"]/genre') yield value as result",
                (r) -> {
                    assertEquals("genre", ((Map) r.get("result")).get("_type"));
                    assertEquals("Fantasy", ((Map) r.get("result")).get("_text"));
                });
    }

    @Test
    public void testLoadXmlXpathReturnBookFromBookTitle () {
        testCall(db, "CALL apoc.load.xml('file:src/test/resources/books.xml', '/catalog/book[title=\"Maeve Ascendant\"]/.') yield value as result",
                (r) -> {
                    Object value = r.values();
                    assertEquals(XML_XPATH_AS_NESTED_MAP, value.toString());
                });
    }

    @Test
    public void testLoadXmlXpathBooKsFromGenre () {
        testResult(db, "CALL apoc.load.xml('file:src/test/resources/books.xml', '/catalog/book[genre=\"Computer\"]') yield value as result",
                (r) -> {
                    Map<String, Object> next = r.next();
                    Object result = next.get("result");
                    Map resultMap = (Map) next.get("result");
                    Object children = resultMap.get("_children");

                    List<Object>  childrenList = (List<Object>) children;
                    assertEquals("bk101", ((Map) result).get("id"));
                    assertEquals("author", ((Map) childrenList.get(0)).get("_type"));
                    assertEquals("Gambardella, Matthew", ((Map) childrenList.get(0)).get("_text"));
                    assertEquals("author", ((Map) childrenList.get(1)).get("_type"));
                    assertEquals("Arciniegas, Fabio", ((Map) childrenList.get(1)).get("_text"));
                    next = r.next();
                    result = next.get("result");
                    resultMap = (Map) next.get("result");
                    children = resultMap.get("_children");
                    childrenList = (List<Object>) children;
                    assertEquals("bk110", ((Map) result).get("id"));
                    assertEquals("author", ((Map) childrenList.get(0)).get("_type"));
                    assertEquals("O'Brien, Tim", ((Map) childrenList.get(0)).get("_text"));
                    assertEquals("title", ((Map) childrenList.get(1)).get("_type"));
                    assertEquals("Microsoft .NET: The Programming Bible", ((Map) childrenList.get(1)).get("_text"));
                    next = r.next();
                    result = next.get("result");
                    resultMap = (Map) next.get("result");
                    children = resultMap.get("_children");
                    childrenList = (List<Object>) children;
                    assertEquals("bk111", ((Map) result).get("id"));
                    assertEquals("author", ((Map) childrenList.get(0)).get("_type"));
                    assertEquals("O'Brien, Tim", ((Map) childrenList.get(0)).get("_text"));
                    assertEquals("title", ((Map) childrenList.get(1)).get("_type"));
                    assertEquals("MSXML3: A Comprehensive Guide", ((Map) childrenList.get(1)).get("_text"));
                    next = r.next();
                    result = next.get("result");
                    resultMap = (Map) next.get("result");
                    children = resultMap.get("_children");
                    childrenList = (List<Object>) children;
                    assertEquals("bk112", ((Map) result).get("id"));
                    assertEquals("author", ((Map) childrenList.get(0)).get("_type"));
                    assertEquals("Galos, Mike", ((Map) childrenList.get(0)).get("_text"));
                    assertEquals("title", ((Map) childrenList.get(1)).get("_type"));
                    assertEquals("Visual Studio 7: A Comprehensive Guide", ((Map) childrenList.get(1)).get("_text"));
                    assertEquals(false, r.hasNext());
                });
    }

    @Test
    public void testLoadXmlViaHdfs () {
        TestUtil.ignoreException(() -> {
            testCall(db, "CALL apoc.load.xml('hdfs://localhost:9000/user/larusba/input/books.xml', '/catalog/book[title=\"Maeve Ascendant\"]/.') yield value as result",
                    (r) -> {
                        Object value = r.values();
                        assertEquals(XML_XPATH_AS_NESTED_MAP, value.toString());
                    });
        }, ConnectException.class);
    }
}