package apoc.util;

import apoc.config.Config;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class FileUtilsRelativePathTest {
    private GraphDatabaseService db;
    private static final File PATH = new File("target/test-data/impermanent-db");
    private static String TEST_FILE = new File(PATH.getAbsolutePath() + "/import/test.csv").toURI().toString();

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder(PATH)
                .setConfig("apoc.import.file.use_neo4j_config", "true")
                .setConfig("dbms.directories.import", "import")
                .setConfig("dbms.security.allow_csv_import_from_file_urls","true")
                .newGraphDatabase();
        TestUtil.registerProcedure(db, Config.class);

    }

    @After
    public void tearDown(){
        db.shutdown();
    }

    @Test
    public void changeNoSlashesUrlWithDirectoryImportContrainedURI() throws Exception {
        assertEquals(TEST_FILE, FileUtils.changeFileUrlIfImportDirectoryConstrained("test.csv"));

    }

    @Test
    public void changeSlashUrlWithDirectoryImportContrainedURI() throws Exception {
        assertEquals(TEST_FILE, FileUtils.changeFileUrlIfImportDirectoryConstrained("/test.csv"));
    }

    @Test
    public void changeFileSlashUrlWithDirectoryImportContrainedURI() throws Exception {
        assertEquals(TEST_FILE, FileUtils.changeFileUrlIfImportDirectoryConstrained("file:/test.csv"));
    }

    @Test
    public void changeFileDoubleSlashesUrlWithdirectoryImportConstrainedURI() throws Exception {
        assertEquals(TEST_FILE, FileUtils.changeFileUrlIfImportDirectoryConstrained("file://test.csv"));
    }

    @Test
    public void changeFileTripleSlashesUrlWithdirectoryImportConstrainedURI() throws Exception {
        assertEquals(TEST_FILE, FileUtils.changeFileUrlIfImportDirectoryConstrained("file:///test.csv"));
    }

    @Test
    public void importDirectoryWithRelativePath() throws Exception {
        assertEquals(TEST_FILE, FileUtils.changeFileUrlIfImportDirectoryConstrained("test.csv"));
    }
}