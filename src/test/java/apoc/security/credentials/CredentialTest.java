package apoc.security.credentials;

import apoc.ApocConfiguration;
import apoc.util.TestUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static org.junit.Assert.*;

public class CredentialTest {

    @Context
    private GraphDatabaseService db;


    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        ApocConfiguration.initialize((GraphDatabaseAPI)db);
        TestUtil.registerProcedure(db, Credential.class);
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testListEmptyCredentials()  {

        testResult(db, "call apoc.security.credentials.list", (result) -> assertFalse(result.hasNext()));
    }

    @Test
    public void testSetCredential() {

        testCall(db, "CALL apoc.security.credentials.set('dbRelKey', 'myUsername', 'myPassword')", (row)->{
            assertEquals("dbRelKey", row.get("key").toString());
            assertEquals("myUsername", row.get("name").toString());
            assertEquals("********", row.get("password").toString());
        });

        testResult(db, "call apoc.security.credentials.get('dbRelKey')", (result) ->{
            assertTrue(result.hasNext());
            Map<String, Object> row = result.next();
            assertEquals("dbRelKey", row.get("key").toString());
            assertEquals("myUsername", row.get("name").toString());
            assertEquals("********", row.get("password").toString());
            assertFalse(result.hasNext());
        });

    }

    @Test(expected = QueryExecutionException.class)
    public void testSetCredentialEmptyParam() {

        testCall(db, "CALL apoc.security.credentials.set('dbRelKey', '', 'myPassword')", (row)-> fail());
    }

    @Test(expected = QueryExecutionException.class)
    public void testSetCredentialNullParam() {

        testCall(db, "CALL apoc.security.credentials.set('dbRelKey', 'myUsername', null)", (row)-> fail());
    }

    @Test
    public void updateCredential() {

        testCall(db, "CALL apoc.security.credentials.set('dbRelKey', 'myUsername', 'myPassword')", (row)-> {
            assertEquals("dbRelKey", row.get("key").toString());
            assertEquals("myUsername", row.get("name").toString());
            assertEquals("********", row.get("password").toString());
        });

        testResult(db, "call apoc.security.credentials.get('dbRelKey')", (result) ->{
            assertTrue(result.hasNext());
            Map<String, Object> row = result.next();
            assertEquals("dbRelKey", row.get("key").toString());
            assertEquals("myUsername", row.get("name").toString());
            assertEquals("********", row.get("password").toString());
            assertFalse(result.hasNext());
        });

        testCall(db, "CALL apoc.security.credentials.set('dbRelKey', 'myNewUsername', 'myPassword')", (row)-> {
            assertEquals("dbRelKey", row.get("key").toString());
            assertEquals("myNewUsername", row.get("name").toString());
            assertEquals("********", row.get("password").toString());
        });

        testResult(db, "call apoc.security.credentials.get('dbRelKey')", (result) ->{
            assertTrue(result.hasNext());
            Map<String, Object> row = result.next();
            assertEquals("dbRelKey", row.get("key").toString());
            assertEquals("myNewUsername", row.get("name").toString());
            assertEquals("********", row.get("password").toString());
            assertFalse(result.hasNext());
        });

    }

    @Test
    public void testListAllCredentials() {
        db.execute("CALL apoc.security.credentials.set('dbRelMySql', 'myUsernameMySql', 'myPasswordMySql')").close();
        db.execute("CALL apoc.security.credentials.set('dbRelPostgres', 'myUsernamePostgres', 'myPasswordPostgres')").close();

        testResult(db, "call apoc.security.credentials.list()", (result) ->{
            assertTrue(result.hasNext());
            Map<String, Object> row = result.next();
            assertEquals("dbRelMySql", row.get("key").toString());
            assertEquals("myUsernameMySql", row.get("name").toString());
            assertEquals("********", row.get("password").toString());
            assertTrue(result.hasNext());
            row = result.next();
            assertEquals("dbRelPostgres", row.get("key").toString());
            assertEquals("myUsernamePostgres", row.get("name").toString());
            assertEquals("********", row.get("password").toString());
            assertFalse(result.hasNext());
        });
    }

    @Test
    public void testGetCredential() {
        db.execute("CALL apoc.security.credentials.set('dbRelKey', 'myUsername', 'myPassword')").close();

        testResult(db, "call apoc.security.credentials.get('dbRelKey')", (result) ->{
            assertTrue(result.hasNext());
            Map<String, Object> row = result.next();
            assertEquals("dbRelKey", row.get("key").toString());
            assertEquals("myUsername", row.get("name").toString());
            assertEquals("********", row.get("password").toString());
            assertFalse(result.hasNext());
        });

        //db.execute("CALL apoc.security.credentials.remove('dbRelKey')").close();
    }

    @Test
    public void testGetNotExistKey() {
        db.execute("CALL apoc.security.credentials.set('dbRelKey', 'myUsername', 'myPassword')").close();

        testResult(db, "call apoc.security.credentials.get('dbRelKeyPostgres')", (result) -> assertFalse(result.hasNext()));

    }

    @Test
    public void testRemoveKey() {
        db.execute("CALL apoc.security.credentials.set('dbRelMySql', 'myUsernameMySql', 'myPasswordMySql')").close();
        db.execute("CALL apoc.security.credentials.set('dbRelPostgres', 'myUsernamePostgres', 'myPasswordPostgres')").close();

        db.execute("CALL apoc.security.credentials.remove('dbRelMySql')").close();

        testResult(db, "call apoc.security.credentials.list()", (result) ->{
            assertTrue(result.hasNext());
            Map<String, Object> row = result.next();
            assertEquals("dbRelPostgres", row.get("key").toString());
            assertEquals("myUsernamePostgres", row.get("name").toString());
            assertEquals("********", row.get("password").toString());
            assertFalse(result.hasNext());
        });

    }

    @Test
    public void testRemoveNotExistKey() {

        ((GraphDatabaseAPI)db).storeId();

        db.execute("CALL apoc.security.credentials.set('dbRelMySql', 'myUsernameMySql', 'myPasswordMySql')").close();
        db.execute("CALL apoc.security.credentials.set('dbRelPostgres', 'myUsernamePostgres', 'myPasswordPostgres')").close();

        db.execute("CALL apoc.security.credentials.remove('dbKeyRel')").close();

        testResult(db, "call apoc.security.credentials.list()", (result) ->{
            assertTrue(result.hasNext());
            Map<String, Object> row = result.next();
            assertEquals("dbRelMySql", row.get("key").toString());
            assertEquals("myUsernameMySql", row.get("name").toString());
            assertEquals("********", row.get("password").toString());
            assertTrue(result.hasNext());
            row = result.next();
            assertEquals("dbRelPostgres", row.get("key").toString());
            assertEquals("myUsernamePostgres", row.get("name").toString());
            assertEquals("********", row.get("password").toString());
            assertFalse(result.hasNext());
        });

    }

}
