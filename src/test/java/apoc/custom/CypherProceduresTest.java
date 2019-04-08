package apoc.custom;

import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author mh
 * @since 18.08.18
 */
public class CypherProceduresTest {

    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, CypherProcedures.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void registerSimpleStatement() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN 42 as answer')");
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    @Ignore
    public void overrideSingleCallStatement() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN 42 as answer')");
        TestUtil.testCall(db, "call custom.answer() yield row return row", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
        String clearCaches = db.execute("call dbms.clearQueryCaches()").resultAsString();
        System.out.println(clearCaches);

        db.execute("call apoc.custom.asProcedure('answer','RETURN 43 as answer')");
        TestUtil.testCall(db, "call custom.answer() yield row return row", (row) -> assertEquals(43L, ((Map)row.get("row")).get("answer")));
    }
    @Test
    @Ignore
    public void overrideCypherCallStatement() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN 42 as answer')");
        TestUtil.testCall(db, "with 1 as foo call custom.answer() yield row return row", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
        db.execute("call dbms.clearQueryCaches()").close();

        db.execute("call apoc.custom.asProcedure('answer','RETURN 43 as answer')");
        TestUtil.testCall(db, "with 1 as foo call custom.answer() yield row return row", (row) -> assertEquals(43L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    public void registerSimpleStatementConcreteResults() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN 42 as answer','read',[['answer','long']])");
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerParameterStatement() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN $answer as answer')");
        TestUtil.testCall(db, "call custom.answer({answer:42})", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    public void registerConcreteParameterStatement() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',null,[['input','number']])");
        TestUtil.testCall(db, "call custom.answer(42)", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
    }

    @Test
    public void registerConcreteParameterAndReturnStatement() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',[['answer','number']],[['input','int','42']])");
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void testAllParameterTypes() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN [$int,$float,$string,$map,$`list int`,$bool,$date,$datetime,$point] as data','read',null," +
                "[['int','int'],['float','float'],['string','string'],['map','map'],['list int','list int'],['bool','bool'],['date','date'],['datetime','datetime'],['point','point']])");
        TestUtil.testCall(db, "call custom.answer(42,3.14,'foo',{a:1},[1],true,date(),datetime(),point({x:1,y:2}))", (row) -> assertEquals(9, ((List)((Map)row.get("row")).get("data")).size()));
    }

    @Test
    public void registerSimpleStatementFunction() throws Exception {
        db.execute("call apoc.custom.asFunction('answer','RETURN 42 as answer')");
        TestUtil.testCall(db, "return custom.answer() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
    }

    @Test
    public void registerSimpleStatementConcreteResultsFunction() throws Exception {
        db.execute("call apoc.custom.asFunction('answer','RETURN 42 as answer','long')");
        TestUtil.testCall(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerSimpleStatementConcreteResultsFunctionUnnamedResultColumn() throws Exception {
        db.execute("call apoc.custom.asFunction('answer','RETURN 42','long')");
        TestUtil.testCall(db, "return custom.answer() as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerParameterStatementFunction() throws Exception {
        db.execute("call apoc.custom.asFunction('answer','RETURN $answer as answer','long')");
        TestUtil.testCall(db, "return custom.answer({answer:42}) as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void registerConcreteParameterAndReturnStatementFunction() throws Exception {
        db.execute("call apoc.custom.asFunction('answer','RETURN $input as answer','long',[['input','number']])");
        TestUtil.testCall(db, "return custom.answer(42) as answer", (row) -> assertEquals(42L, row.get("answer")));
    }

    @Test
    public void testAllParameterTypesFunction() throws Exception {
        db.execute("call apoc.custom.asFunction('answer','RETURN [$int,$float,$string,$map,$`list int`,$bool,$date,$datetime,$point] as data','list of any'," +
                "[['int','int'],['float','float'],['string','string'],['map','map'],['list int','list int'],['bool','bool'],['date','date'],['datetime','datetime'],['point','point']], true)");
        TestUtil.testCall(db, "return custom.answer(42,3.14,'foo',{a:1},[1],true,date(),datetime(),point({x:1,y:2})) as data", (row) -> assertEquals(9, ((List)row.get("data")).size()));
    }

    @Test
    public void registerSimpleStatementWithDescription() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN 42 as answer', 'read', null, null, 'Answer to the Ultimate Question of Life, the Universe, and Everything')");
        TestUtil.testCall(db, "call custom.answer()", (row) -> assertEquals(42L, ((Map)row.get("row")).get("answer")));
        Map<String, Map<String, Object>> procedures = CypherProcedures.CustomProcedureStorage.list((GraphDatabaseAPI) db).get(CypherProcedures.PROCEDURES);
        assertEquals("Answer to the Ultimate Question of Life, the Universe, and Everything", procedures.get("answer").get("description"));
    }

    @Test
    public void registerSimpleStatementFunctionDescription() throws Exception {
        db.execute("call apoc.custom.asFunction('answer','RETURN 42 as answer', '', null, false, 'Answer to the Ultimate Question of Life, the Universe, and Everything')");
        TestUtil.testCall(db, "return custom.answer() as row", (row) -> assertEquals(42L, ((Map)((List)row.get("row")).get(0)).get("answer")));
        Map<String, Map<String, Object>> functions = CypherProcedures.CustomProcedureStorage.list((GraphDatabaseAPI) db).get(CypherProcedures.FUNCTIONS);
        assertEquals("Answer to the Ultimate Question of Life, the Universe, and Everything", functions.get("answer").get("description"));
    }

    @Test
    public void listAllProceduresAndFunctions() throws Exception {
        db.execute("call apoc.custom.asProcedure('answer','RETURN $input as answer','read',[['answer','number']],[['input','int','42']], 'Answer to the Ultimate Question of Life, the Universe, and Everything')");
        db.execute("call apoc.custom.asFunction('answer','RETURN $input as answer','long', [['input','number']], false, 'Answer to the Ultimate Question of Life, the Universe, and Everything')");
        TestUtil.testResult(db, "call apoc.custom.list", (row) -> {
            assertTrue(row.hasNext());
            while (row.hasNext()){
                Map<String, Object> value = row.next();
                assertTrue(value.containsKey("type"));
                assertTrue("function".equals(value.get("type")) || "procedure".equals(value.get("type")));

                if("procedure".equals(value.get("type"))){
                    assertEquals("answer", value.get("name"));
                    assertEquals("[[answer, number]]", value.get("outputs").toString());
                    assertEquals("[[input, int, 42]]", value.get("inputs").toString());
                    assertEquals("Answer to the Ultimate Question of Life, the Universe, and Everything", value.get("description").toString());
                }

                if("function".equals(value.get("type"))){
                    assertEquals("answer", value.get("name"));
                    assertEquals("long", value.get("outputs").toString());
                    assertEquals("[[input, number]]", value.get("inputs").toString());
                    assertEquals("Answer to the Ultimate Question of Life, the Universe, and Everything", value.get("description").toString());
                }
            }
            
        });
    }

    @Test
    public void listAllWithNoProceduresAndFunctions() throws Exception {
        db.execute("call apoc.custom.list");
        TestUtil.testResult(db, "call apoc.custom.list", (row) ->
                assertFalse(row.hasNext())
        );
    }
}
