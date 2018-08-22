package apoc.util;

import apoc.config.Config;
import apoc.load.relative.LoadCsvRelativePathTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class FileUtilsAbsolutePathTest {
    private GraphDatabaseService db;
    private static String testFile = new File(LoadCsvRelativePathTest.class.getClassLoader().getResource("test.csv").getPath()).toURI().toString();

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .setConfig("apoc.import.file.use_neo4j_config", "true")
                .setConfig("dbms.security.allow_csv_import_from_file_urls","true")
                .setConfig("foo", "bar").setConfig("foouri", "baruri")
                .setConfig("foopass", "foopass").setConfig("foo.credentials", "foo.credentials").newGraphDatabase();
        TestUtil.registerProcedure(db, Config.class);
    }

    @After
    public void tearDown(){
        db.shutdown();
    }

    @Test
    public void notChangeFileUrlWithdirectoryImportConstrainedURI() throws Exception {
        assertEquals(testFile, FileUtils.changeFileUrlIfImportDirectoryConstrained(new File("out/test/resources/test.csv").getAbsolutePath()));
    }

    @Test
    public void notChangeFileUrlWithdirectoryAndProtocollImportConstrainedURI() throws Exception {
        assertEquals(testFile, FileUtils.changeFileUrlIfImportDirectoryConstrained("file:///"+new File("out/test/resources/test.csv").getAbsolutePath()));
    }
}