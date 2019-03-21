package apoc.periodic;

import apoc.load.Jdbc;
import apoc.util.MapUtil;
import apoc.util.TestUtil;
import org.junit.*;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static apoc.util.Util.map;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.DependencyResolver.SelectionStrategy.FIRST;

public class PeriodicTest {

    public static final long RUNDOWN_COUNT = 1000;
    public static final int BATCH_SIZE = 399;
    private GraphDatabaseService db;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        TestUtil.registerProcedure(db, Periodic.class, Jdbc.class);
        db.execute("call apoc.periodic.list() yield name call apoc.periodic.cancel(name) yield name as name2 return count(*)").close();
    }

    @After
    public void tearDown() {
        db.shutdown();
    }

    @Test
    public void testSubmitStatement() throws Exception {
        String callList = "CALL apoc.periodic.list()";
        // force pre-caching the queryplan
        System.out.println("call list" + db.execute(callList).resultAsString());
        assertFalse(db.execute(callList).hasNext());

        testCall(db, "CALL apoc.periodic.submit('foo','create (:Foo)')",
                (row) -> {
                    assertEquals("foo", row.get("name"));
                    assertEquals(false, row.get("done"));
                    assertEquals(false, row.get("cancelled"));
                    assertEquals(0L, row.get("delay"));
                    assertEquals(0L, row.get("rate"));
                });

        long count = tryReadCount(50, "MATCH (:Foo) RETURN COUNT(*) AS count", 1L);

        assertThat(String.format("Expected %d, got %d ", 1L, count), count, equalTo(1L));

        testCall(db, callList, (r) -> assertEquals(true, r.get("done")));
    }

    @Test
    public void testTerminateCommit() throws Exception {
        testTerminatePeriodicQuery("CALL apoc.periodic.commit('UNWIND range(0,1000) as id WITH id CREATE (:Foo {id: id}) limit 1000', {})");
    }

    @Test(expected = QueryExecutionException.class)
    public void testPeriodicCommitWithoutLimitShouldFail() {
        db.execute("CALL apoc.periodic.commit('return 0')");
    }

    @Test
    public void testRunDown() throws Exception {
        db.execute("UNWIND range(1,{count}) AS id CREATE (n:Person {id:id})", MapUtil.map("count", RUNDOWN_COUNT)).close();

        String query = "MATCH (p:Person) WHERE NOT p:Processed WITH p LIMIT {limit} SET p:Processed RETURN count(*)";

        testCall(db, "CALL apoc.periodic.commit({query},{params})", MapUtil.map("query", query, "params", MapUtil.map("limit", BATCH_SIZE)), r -> {
            assertEquals((long) Math.ceil((double) RUNDOWN_COUNT / BATCH_SIZE), r.get("executions"));
            assertEquals(RUNDOWN_COUNT, r.get("updates"));
        });

        ResourceIterator<Long> it = db.execute("MATCH (p:Processed) RETURN COUNT(*) AS c").<Long>columnAs("c");
        long count = it.next();
        it.close();
        assertEquals(RUNDOWN_COUNT, count);

    }

    @Test
    public void testRock_n_roll() throws Exception {
        // setup
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();

        // when&then

        // TODO: remove forcing rule based in the 2nd kernelTransaction next line when 3.0.2 is released, due to https://github.com/neo4j/neo4j/pull/7152
        testResult(db, "CALL apoc.periodic.rock_n_roll('match (p:Person) return p', 'WITH {p} as p SET p.lastname =p.name REMOVE p.name', 10)", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
        });
        // then
        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count"))
        );
    }

    @Test
    public void testTerminateRockNRoll() throws Exception {
        testTerminatePeriodicQuery("CALL apoc.periodic.rock_n_roll('UNWIND range(0,1000) as id RETURN id', 'CREATE (:Foo {id: $id})', 10)");
    }

    public void testTerminatePeriodicQuery(String periodicQuery) {
        killPeriodicQueryAsync();
        try {
            testResult(db, periodicQuery, result -> {
                Map<String, Object> row = Iterators.single(result);
                assertEquals( periodicQuery + " result: " + row.toString(), true, row.get("wasTerminated"));
            });
            fail("Should have terminated");
        } catch(Exception tfe) {
            assertEquals(tfe.getMessage(),true, tfe.getMessage().contains("terminated"));
        }
    }

    private final static String KILL_PERIODIC_QUERY = "call dbms.listQueries() yield queryId, query, status\n" +
            "with * where query contains ('apoc.' + 'periodic')\n" +
            "call dbms.killQuery(queryId) yield queryId as killedId\n" +
            "return killedId";


    public void killPeriodicQueryAsync() {
        new Thread(() -> {
            int retries = 10;
            try {
                while (retries-- > 0 && !terminateQuery("apoc.periodic")) {
                    Thread.sleep(10);
                }
            } catch (InterruptedException e) {
                // ignore
            }
        }).start();
    }

    boolean terminateQuery(String pattern) {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) db).getDependencyResolver();
        EmbeddedProxySPI nodeManager = dependencyResolver.resolveDependency( EmbeddedProxySPI.class, FIRST );
        KernelTransactions kernelTransactions = dependencyResolver.resolveDependency(KernelTransactions.class, FIRST);

        long numberOfKilledTransactions = kernelTransactions.activeTransactions().stream()
                .filter(kernelTransactionHandle ->
                    kernelTransactionHandle.executingQueries().anyMatch(
                            executingQuery -> executingQuery.queryText().contains(pattern)
                    )
                )
                .map(kernelTransactionHandle -> kernelTransactionHandle.markForTermination(Status.Transaction.Terminated))
                .count();
        return numberOfKilledTransactions > 0;
    }

    @Test
    public void testIterateErrors() throws Exception {
        testResult(db, "CALL apoc.periodic.rock_n_roll('UNWIND range(0,99) as id RETURN id', 'CREATE (:Foo {id: 1 / ($id % 10)})', 10)", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
            assertEquals(0L, row.get("committedOperations"));
            assertEquals(10L, row.get("failedOperations"));
            assertEquals(10L, row.get("failedBatches"));
            Map<String, Object> batchErrors = map("org.neo4j.graphdb.TransactionFailureException: Transaction was marked as successful, but unable to commit transaction so rolled back.", 10L);
            assertEquals(batchErrors, ((Map) row.get("batch")).get("errors"));
            Map<String, Object> operationsErrors = map("/ by zero", 10L);
            assertEquals(operationsErrors, ((Map) row.get("operations")).get("errors"));
        });
    }

    @Test
    public void testPeriodicIterateErrors() throws Exception {
        testResult(db, "CALL apoc.periodic.iterate('UNWIND range(0,99) as id RETURN id', 'CREATE null', {batchSize:10,iterateList:true})", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
            assertEquals(0L, row.get("committedOperations"));
            assertEquals(100L, row.get("failedOperations"));
            assertEquals(10L, row.get("failedBatches"));
            Map<String, Object> batchErrors = map("org.neo4j.graphdb.TransactionFailureException: Transaction was marked as successful, but unable to commit transaction so rolled back.", 10L);
            assertEquals(batchErrors, ((Map) row.get("batch")).get("errors"));
            Map<String, Object> operationsErrors = map("Parentheses are required to identify nodes in patterns, i.e. (null) (line 1, column 56 (offset: 55))\n" +
                    "\"UNWIND {_batch} AS _batch WITH _batch.id AS id  CREATE null\"\n" +
                    "                                                        ^", 10L);
            assertEquals(operationsErrors, ((Map) row.get("operations")).get("errors"));
        });
    }

    @Test
    public void testTerminateIterate() throws Exception {
        testTerminatePeriodicQuery("CALL apoc.periodic.iterate('UNWIND range(0,1000) as id RETURN id', 'WITH $id as id CREATE (:Foo {id: $id})', {batchSize:1,parallel:true})");
        testTerminatePeriodicQuery("CALL apoc.periodic.iterate('UNWIND range(0,1000) as id RETURN id', 'WITH $id as id CREATE (:Foo {id: $id})', {batchSize:10,iterateList:true})");
        testTerminatePeriodicQuery("CALL apoc.periodic.iterate('UNWIND range(0,1000) as id RETURN id', 'WITH $id as id CREATE (:Foo {id: $id})', {batchSize:10,iterateList:false})");
    }

    @Test
    public void testIteratePrefixGiven() throws Exception {
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();

        testResult(db, "CALL apoc.periodic.iterate('match (p:Person) return p', 'WITH {p} as p SET p.lastname =p.name REMOVE p.name', {batchSize:10,parallel:true})", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
        });

        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count"))
        );
    }

    @Test
    public void testIterate() throws Exception {
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();

        testResult(db, "CALL apoc.periodic.iterate('match (p:Person) return p', 'SET p.lastname =p.name REMOVE p.name', {batchSize:10,parallel:true})", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
        });

        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count"))
        );
    }

    @Test
    public void testIteratePrefix() throws Exception {
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();

        testResult(db, "CALL apoc.periodic.iterate('match (p:Person) return p', 'SET p.lastname =p.name REMOVE p.name', {batchSize:10,parallel:true})", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
        });

        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count"))
        );
    }

    @Test
    public void testIterateBatch() throws Exception {
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();

        testResult(db, "CALL apoc.periodic.iterate('match (p:Person) return p', 'SET p.lastname = p.name REMOVE p.name', {batchSize:10, iterateList:true, parallel:true})", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
        });

        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count"))
        );
    }

    @Test
    public void testIterateBatchPrefix() throws Exception {
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();

        testResult(db, "CALL apoc.periodic.iterate('match (p:Person) return p', 'SET p.lastname = p.name REMOVE p.name', {batchSize:10, iterateList:true, parallel:true})", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
        });

        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count"))
        );
    }

    @Test
    public void testIterateRetries() throws Exception {
        testResult(db, "CALL apoc.periodic.iterate('return 1', 'CREATE (n {prop: 1/{_retry}})', {retries:1})", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(1L, row.get("batches"));
            assertEquals(1L, row.get("total"));
            assertEquals(1L, row.get("retries"));
        });
    }

    @Test
    public void testIterateFail() throws Exception {
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();
        testResult(db, "CALL apoc.periodic.iterate('match (p:Person) return p', 'WITH {p} as p SET p.lastname = p.name REMOVE x.name', {batchSize:10,parallel:true})", result -> {
            Map<String, Object> row = Iterators.single(result);
            assertEquals(10L, row.get("batches"));
            assertEquals(100L, row.get("total"));
            assertEquals(100L, row.get("failedOperations"));
            assertEquals(0L, row.get("committedOperations"));
        });

        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(0L, row.get("count"))
        );
    }

    @Test
    public void testIterateJDBC() throws Exception {
        TestUtil.ignoreException(() -> {
            testResult(db, "CALL apoc.periodic.iterate('call apoc.load.jdbc(\"jdbc:mysql://localhost:3306/northwind?user=root\",\"customers\")', 'create (c:Customer) SET c += {row}', {batchSize:10,parallel:true})", result -> {
                Map<String, Object> row = Iterators.single(result);
                assertEquals(3L, row.get("batches"));
                assertEquals(29L, row.get("total"));
            });

            testCall(db,
                    "MATCH (p:Customer) return count(p) as count",
                    row -> assertEquals(29L, row.get("count"))
            );
        }, SQLException.class);
    }

    @Test
    public void testRock_n_roll_while() throws Exception {
        // setup
        db.execute("UNWIND range(1,100) AS x CREATE (:Person{name:'Person_'+x})").close();

        // when&then
        testResult(db, "CALL apoc.periodic.rock_n_roll_while('return coalesce({previous},3)-1 as loop', 'match (p:Person) return p', 'MATCH (p) where p={p} SET p.lastname =p.name', 10)", result -> {
            long l = 0;
            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                assertEquals(2L - l, row.get("loop"));
                assertEquals(10L, row.get("batches"));
                assertEquals(100L, row.get("total"));
                l += 1;
            }
            assertEquals(2L, l);
        });
        // then
        testCall(db,
                "MATCH (p:Person) where p.lastname is not null return count(p) as count",
                row -> assertEquals(100L, row.get("count"))
        );
    }

    @Test
    public void testCountdown() {
        int startValue = 10;
        int rate = 1;

        db.execute("CREATE (counter:Counter {c: " + startValue + "})");
        String statementToRepeat = "MATCH (counter:Counter) SET counter.c = counter.c - 1 RETURN counter.c as count";

        Map<String, Object> params = map("kernelTransaction", statementToRepeat, "rate", rate);
        testResult(db, "CALL apoc.periodic.countdown('decrement',{kernelTransaction}, {rate})", params, r -> {
            try {
                // Number of iterations per rate (in seconds)
                Thread.sleep(startValue * rate * 1000);
            } catch (InterruptedException e) {

            }

            Map<String, Object> result = db.execute("MATCH (counter:Counter) RETURN counter.c as c").next();
            assertEquals(0L, result.get("c"));
        });
    }

    @Test
    public void testRepeatParams() {
        db.execute("CALL apoc.periodic.repeat('repeat-params', 'MERGE (person:Person {name: {nameValue}})', 2, {params: {nameValue: 'John Doe'}} ) YIELD name RETURN name" );
        try {
            Thread.sleep(3000);
            testCall(db, "CALL apoc.periodic.list()", row -> {
               assertNotNull(row.get("started"));
               assertNotNull(row.get("lastSuccessful"));
               assertEquals(-1L, row.get("lastFailure"));
           });

        } catch (InterruptedException e) {

        }

        testCall(db,
                "MATCH (p:Person {name: 'John Doe'}) RETURN p.name AS name",
                row -> assertEquals( row.get( "name" ), "John Doe" )
        );
    }

    @Test
    public void testRepeatRetryOnFailFalse() {

        db.execute(
                "CALL apoc.periodic.repeat('repeat-retry-on-fail-false', 'MERGE (lastTimestamp:timestamp) ON CREATE SET lastTimestamp.timestamp = 0 WITH lastTimestamp CALL apoc.load.jdbc(\"jdbc:sqlserver://localhost:1433;databaseName=TestDatabase\",\"SELECT * FROM PERSON\") YIELD row RETURN row', 2, {params:{}} )" );
        try {
            Thread.sleep(3000);
            testCall(db, "CALL apoc.periodic.list()", row -> {
                assertEquals(-1L, row.get("lastFailure"));
                assertNotNull(row.get("started"));
                assertEquals(0L, row.get("failures"));
            });
        } catch (InterruptedException e) {

        }

        testResult(db,
                "MATCH (p:Person) RETURN p", row -> {
            assertNotNull(row);
            assertFalse(row.hasNext());
        });
    }

    @Test
    public void testRepeatRetryOnFailTrue() {

        db.execute(
                "CALL apoc.periodic.repeat('repeat-retry-on-fail-true', 'MERGE (lastTimestamp:timestamp) ON CREATE SET lastTimestamp.timestamp = 0 WITH lastTimestamp CALL apoc.load.jdbc(\"jdbc:sqlserver://localhost:1433;databaseName=TestDatabase\",\"SELECT * FROM PERSON\") YIELD row RETURN row', 3, {params:{}, retryOnError: true} )" );
        try {
            Thread.sleep(3000);
            Map<String, Object> result1 = db.execute("CALL apoc.periodic.list()").next();
            assertNotNull(result1.get("started"));
            assertTrue((Long)result1.get("failures") > 0L);
            assertEquals(1L, result1.get("failures"));
            assertNotNull(result1.get("lastFailure"));

            Thread.sleep(3000);
            Map<String, Object> result2 = db.execute("CALL apoc.periodic.list()").next();
            assertNotNull(result2.get("started"));
            assertEquals(result1.get("started"), result2.get("started"));
            assertTrue((Long)result2.get("failures") > 1L);
            assertTrue((Long)result2.get("failures") > (Long)result1.get("failures"));
            assertEquals(2L, result2.get("failures"));
            assertNotNull(result2.get("lastFailure"));
            assertTrue((Long)result2.get("lastFailure") > (Long)result1.get("lastFailure"));

        } catch (InterruptedException e) {

        }

        testResult(db,
                "MATCH (p:Person) RETURN p", row -> {
                    assertNotNull(row);
                    assertFalse(row.hasNext());
                });
    }

    private long tryReadCount(int maxAttempts, String statement, long expected) throws InterruptedException {
        int attempts = 0;
        long count;
        do {
            Thread.sleep(100);
            attempts++;
            count = readCount(statement);
        } while (attempts < maxAttempts && count != expected);
        return count;
    }

    private long readCount(String statement) {
        try (ResourceIterator<Long> it = db.execute(statement).columnAs("count")) {
            return Iterators.single(it);
        }
    }

    @Test
    public void testPeriodicJobStoragePersist(){
        db.execute("CALL apoc.periodic.repeat('repeat-params', 'MERGE (person:Person {name: {nameValue}})', 2, {params: {nameValue: 'John Doe'}, persist : true} ) YIELD name RETURN name" );

        Map<String, Map<String, Map<String, Object>>> apocPeriodicJobs = Periodic.JobStorage.list(Periodic.JobStorage.getProperties((GraphDatabaseAPI) db));
        assertNotNull(apocPeriodicJobs);
        assertFalse(apocPeriodicJobs.isEmpty());

        Map<String, Map<String, Object>> jobs = apocPeriodicJobs.get(Periodic.JobStorage.JOBS);
        assertNotNull(jobs);
        assertFalse(jobs.isEmpty());
        assertTrue(jobs.containsKey("repeat-params"));

        Map<String, Object> dataJob = jobs.get("repeat-params");
        assertNotNull(dataJob);
        assertFalse(dataJob.isEmpty());
        assertTrue(dataJob.containsKey("statement"));
        assertTrue(dataJob.containsKey("rate"));
        assertTrue(dataJob.containsKey("config"));
        assertEquals("MERGE (person:Person {name: {nameValue}})", dataJob.get("statement"));
        assertEquals(2L, dataJob.get("rate"));

        Map<String, Object> params = new HashMap<>();
        params.put("nameValue", "John Doe");
        Map<String, Object> config = new HashMap<>();
        config.put("params", params);
        config.put("persist", Boolean.TRUE);

        assertEquals(config, dataJob.get("config"));

        Periodic.JobStorage.remove((GraphDatabaseAPI) db, "repeat-params");
    }

    @Test
    public void testPeriodicJobStorageRemove(){
        db.execute("CALL apoc.periodic.repeat('repeat-params', 'MERGE (person:Person {name: {nameValue}})', 2, {params: {nameValue: 'John Doe'}, persist : true} ) YIELD name RETURN name" );

        Map<String, Map<String, Map<String, Object>>> apocPeriodicJobsBeforeRemove = Periodic.JobStorage.list(Periodic.JobStorage.getProperties((GraphDatabaseAPI) db));
        assertNotNull(apocPeriodicJobsBeforeRemove);
        assertFalse(apocPeriodicJobsBeforeRemove.isEmpty());

        Map<String, Map<String, Object>> jobsBeforeRemove = apocPeriodicJobsBeforeRemove.get(Periodic.JobStorage.JOBS);
        assertNotNull(jobsBeforeRemove);
        assertTrue(jobsBeforeRemove.containsKey("repeat-params"));

        Map<String, Object> dataJobBeforeRemove = jobsBeforeRemove.get("repeat-params");
        assertNotNull(dataJobBeforeRemove);
        assertFalse(dataJobBeforeRemove.isEmpty());
        assertTrue(dataJobBeforeRemove.containsKey("statement"));
        assertTrue(dataJobBeforeRemove.containsKey("rate"));
        assertTrue(dataJobBeforeRemove.containsKey("config"));

        assertEquals("MERGE (person:Person {name: {nameValue}})", dataJobBeforeRemove.get("statement"));
        assertEquals(2L, dataJobBeforeRemove.get("rate"));
        Map<String, Object> params = new HashMap<>();
        params.put("nameValue", "John Doe");

        Map<String, Object> config = new HashMap<>();
        config.put("params", params);
        config.put("persist", Boolean.TRUE);
        assertEquals(config, dataJobBeforeRemove.get("config"));

        Periodic.JobStorage.remove((GraphDatabaseAPI) db, "repeat-params");
        Map<String, Map<String, Map<String, Object>>> apocPeriodicAfterRemove = Periodic.JobStorage.list(Periodic.JobStorage.getProperties((GraphDatabaseAPI) db));
        assertNotNull(apocPeriodicAfterRemove);
        assertFalse(apocPeriodicAfterRemove.isEmpty());

        Map<String, Map<String, Object>> jobsAfterRemove = apocPeriodicAfterRemove.get(Periodic.JobStorage.JOBS);
        assertNotNull(jobsAfterRemove);
        assertTrue(jobsAfterRemove.isEmpty());

    }

    @Test
    public void testPeriodicJobStorageUpdate(){
        db.execute("CALL apoc.periodic.repeat('repeat-params', 'MERGE (person:Person {name: {nameValue}})', 2, {params: {nameValue: 'John Doe'}, persist : true} ) YIELD name RETURN name" );

        Map<String, Map<String, Map<String, Object>>> apocPeriodicJobsBeforeUpdate = Periodic.JobStorage.list(Periodic.JobStorage.getProperties((GraphDatabaseAPI) db));
        assertNotNull(apocPeriodicJobsBeforeUpdate);
        assertFalse(apocPeriodicJobsBeforeUpdate.isEmpty());

        Map<String, Map<String, Object>> jobsBeforeUpdate = apocPeriodicJobsBeforeUpdate.get(Periodic.JobStorage.JOBS);
        assertNotNull(jobsBeforeUpdate);
        assertTrue(jobsBeforeUpdate.containsKey("repeat-params"));

        Map<String, Object> dataJobBeforeUpdate = jobsBeforeUpdate.get("repeat-params");
        assertNotNull(dataJobBeforeUpdate);
        assertFalse(dataJobBeforeUpdate.isEmpty());
        assertTrue(dataJobBeforeUpdate.containsKey("statement"));
        assertTrue(dataJobBeforeUpdate.containsKey("rate"));
        assertTrue(dataJobBeforeUpdate.containsKey("config"));

        assertEquals("MERGE (person:Person {name: {nameValue}})", dataJobBeforeUpdate.get("statement"));
        assertEquals(2L, dataJobBeforeUpdate.get("rate"));

        Map<String, Object> params = new HashMap<>();
        params.put("nameValue", "John Doe");
        Map<String, Object> config = new HashMap<>();
        config.put("params", params);
        config.put("persist", Boolean.TRUE);
        assertEquals(config, dataJobBeforeUpdate.get("config"));

        params.remove("nameValue");
        params.put("valueName", "Doe John");
        config.put("params", params);

        Periodic.JobStorage.persist((GraphDatabaseAPI) db, "repeat-params", "MERGE (person:Person {name: {valueName}})", 3, config);
        Map<String, Map<String, Map<String, Object>>> apocPeriodicAfterUpdate = Periodic.JobStorage.list(Periodic.JobStorage.getProperties((GraphDatabaseAPI) db));
        assertNotNull(apocPeriodicAfterUpdate);
        assertFalse(apocPeriodicAfterUpdate.isEmpty());

        Map<String, Map<String, Object>> jobsAfterUpdate = apocPeriodicAfterUpdate.get(Periodic.JobStorage.JOBS);
        assertNotNull(jobsAfterUpdate);
        assertFalse(jobsAfterUpdate.isEmpty());

        Map<String, Object> dataJobAfterUpdate = jobsAfterUpdate.get("repeat-params");
        assertNotNull(dataJobAfterUpdate);
        assertFalse(dataJobAfterUpdate.isEmpty());
        assertTrue(dataJobAfterUpdate.containsKey("statement"));
        assertTrue(dataJobAfterUpdate.containsKey("rate"));
        assertTrue(dataJobAfterUpdate.containsKey("config"));

        assertEquals("MERGE (person:Person {name: {valueName}})", dataJobAfterUpdate.get("statement"));
        assertEquals(3L, dataJobAfterUpdate.get("rate"));

        assertEquals(config, dataJobAfterUpdate.get("config"));

        Periodic.JobStorage.remove((GraphDatabaseAPI) db, "repeat-params");
    }
}
