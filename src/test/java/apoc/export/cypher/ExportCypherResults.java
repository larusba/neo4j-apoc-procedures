package apoc.export.cypher;

import static apoc.export.util.ExportFormat.*;

class ExportCypherResults {

    static final String EXPECTED_NODES = String.format("BEGIN%n" +
            "CREATE (:`Foo`:`UNIQUE IMPORT LABEL` {`born`:date('2018-10-31'), `name`:\"foo\", `UNIQUE IMPORT ID`:0});%n" +
            "CREATE (:`Bar` {`age`:42, `name`:\"bar\"});%n" +
            "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`age`:12, `UNIQUE IMPORT ID`:2});%n" +
            "COMMIT%n");

    private static final String EXPECTED_NODES_MERGE = String.format("BEGIN%n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}) SET n.`name`=\"foo\", n.`born`=date('2018-10-31'), n:`Foo`;%n" +
            "MERGE (n:`Bar`{`name`:\"bar\"}) SET n.`age`=42;%n" +
            "MERGE (n:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:2}) SET n.`age`=12, n:`Bar`;%n" +
            "COMMIT%n");

    static final String EXPECTED_NODES_MERGE_ON_CREATE_SET =
            EXPECTED_NODES_MERGE.replaceAll(" SET ", " ON CREATE SET ");

    static final String EXPECTED_NODES_EMPTY = String.format("BEGIN%n" +
            "COMMIT%n");

    static final String EXPECTED_SCHEMA = String.format("BEGIN%n" +
            "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
            "CREATE INDEX ON :`Foo`(`name`);%n" +
            "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
            "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
            "COMMIT%n" +
            "SCHEMA AWAIT%n");

    static final String EXPECTED_SCHEMA_EMPTY = String.format("BEGIN%n" +
            "COMMIT%n" +
            "SCHEMA AWAIT%n");

    private static final String EXPECTED_INDEXES_AWAIT = String.format("CALL db.awaitIndex(':`Foo`(`name`)');%n" +
            "CALL db.awaitIndex(':`Bar`(`first_name`,`last_name`)');%n" +
            "CALL db.awaitIndex(':`Bar`(`name`)');%n");

    static final String EXPECTED_RELATIONSHIPS = String.format("BEGIN%n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`Bar`{`name`:\"bar\"}) CREATE (n1)-[r:`KNOWS` {`since`:2016}]->(n2);%n" +
            "COMMIT%n");

    private static final String EXPECTED_RELATIONSHIPS_MERGE = String.format("BEGIN%n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:0}), (n2:`Bar`{`name`:\"bar\"}) MERGE (n1)-[r:`KNOWS`]->(n2) SET r.`since`=2016;%n" +
            "COMMIT%n");

    static final String EXPECTED_RELATIONSHIPS_MERGE_ON_CREATE_SET =
            EXPECTED_RELATIONSHIPS_MERGE.replaceAll(" SET ", " ON CREATE SET ");

    static final String EXPECTED_CLEAN_UP = String.format("BEGIN%n" +
            "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
            "COMMIT%n");

    static final String EXPECTED_CLEAN_UP_EMPTY = String.format("BEGIN%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "COMMIT%n");

    static final String EXPECTED_ONLY_SCHEMA_NEO4J_SHELL = String.format("BEGIN%n" +
            "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
            "CREATE INDEX ON :`Foo`(`name`);%n" +
            "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
            "COMMIT%n" +
            "SCHEMA AWAIT%n");

    static final String EXPECTED_CYPHER_POINT = String.format("BEGIN%n" +
            "CREATE (:`Test`:`UNIQUE IMPORT LABEL` {`name`:\"foo\", `place2d`:point({x: 2.3, y: 4.5, crs: 'cartesian'}), `place3d1`:point({x: 2.3, y: 4.5, z: 1.2, crs: 'cartesian-3d'}), `UNIQUE IMPORT ID`:20});%n" +
            "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`place3d`:point({x: 12.78, y: 56.7, z: 100.0, crs: 'wgs-84-3d'}), `UNIQUE IMPORT ID`:21});%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
            "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
            "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
            "COMMIT%n" +
            "SCHEMA AWAIT%n" +
            "BEGIN%n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:20}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:21}) CREATE (n1)-[r:`FRIEND_OF` {`place2d`:point({x: 56.7, y: 12.78, crs: 'wgs-84'})}]->(n2);%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
            "COMMIT%n");

    static final String EXPECTED_CYPHER_DATE = String.format("BEGIN%n" +
            "CREATE (:`Test`:`UNIQUE IMPORT LABEL` {`date`:date('2018-10-30'), `datetime`:datetime('2018-10-30T12:50:35.556+01:00'), `localTime`:localdatetime('2018-10-30T19:32:24'), `name`:\"foo\", `UNIQUE IMPORT ID`:20});%n" +
            "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`datetime`:datetime('2018-10-30T12:50:35.556Z'), `UNIQUE IMPORT ID`:21});%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
            "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
            "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
            "COMMIT%n" +
            "SCHEMA AWAIT%n" +
            "BEGIN%n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:20}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:21}) CREATE (n1)-[r:`FRIEND_OF` {`date`:date('2018-10-30')}]->(n2);%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
            "COMMIT%n");

    static final String EXPECTED_CYPHER_TIME = String.format("BEGIN%n" +
            "CREATE (:`Test`:`UNIQUE IMPORT LABEL` {`local`:localtime('12:50:35.556'), `name`:\"foo\", `t`:time('12:50:35.556+01:00'), `UNIQUE IMPORT ID`:20});%n" +
            "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`datetime`:datetime('2018-10-30T12:50:35.556+01:00'), `UNIQUE IMPORT ID`:21});%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
            "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
            "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
            "COMMIT%n" +
            "SCHEMA AWAIT%n" +
            "BEGIN%n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:20}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:21}) CREATE (n1)-[r:`FRIEND_OF` {`t`:time('12:50:35.556+01:00')}]->(n2);%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
            "COMMIT%n");

    static final String EXPECTED_CYPHER_DURATION = String.format("BEGIN%n" +
            "CREATE (:`Test`:`UNIQUE IMPORT LABEL` {`duration`:duration('P5M1DT12H'), `name`:\"foo\", `UNIQUE IMPORT ID`:20});%n" +
            "CREATE (:`Bar`:`UNIQUE IMPORT LABEL` {`duration`:duration('P5M1DT12H'), `UNIQUE IMPORT ID`:21});%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "CREATE INDEX ON :`Bar`(`first_name`,`last_name`);%n" +
            "CREATE CONSTRAINT ON (node:`Bar`) ASSERT node.`name` IS UNIQUE;%n" +
            "CREATE CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
            "COMMIT%n" +
            "SCHEMA AWAIT%n" +
            "BEGIN%n" +
            "MATCH (n1:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:20}), (n2:`UNIQUE IMPORT LABEL`{`UNIQUE IMPORT ID`:21}) CREATE (n1)-[r:`FRIEND_OF` {`duration`:duration('P5M1DT12H')}]->(n2);%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "MATCH (n:`UNIQUE IMPORT LABEL`)  WITH n LIMIT 20000 REMOVE n:`UNIQUE IMPORT LABEL` REMOVE n.`UNIQUE IMPORT ID`;%n" +
            "COMMIT%n" +
            "BEGIN%n" +
            "DROP CONSTRAINT ON (node:`UNIQUE IMPORT LABEL`) ASSERT node.`UNIQUE IMPORT ID` IS UNIQUE;%n" +
            "COMMIT%n");

    static final String EXPECTED_NEO4J_SHELL = EXPECTED_NODES + EXPECTED_SCHEMA + EXPECTED_RELATIONSHIPS + EXPECTED_CLEAN_UP;

    static final String EXPECTED_CYPHER_SHELL = EXPECTED_NEO4J_SHELL
            .replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
            .replace(NEO4J_SHELL.commit(),CYPHER_SHELL.commit())
            .replace(NEO4J_SHELL.schemaAwait(), EXPECTED_INDEXES_AWAIT)
            .replace(NEO4J_SHELL.schemaAwait(),CYPHER_SHELL.schemaAwait());

    static final String EXPECTED_PLAIN = EXPECTED_NEO4J_SHELL
            .replace(NEO4J_SHELL.begin(), PLAIN_FORMAT.begin()).replace(NEO4J_SHELL.commit(), PLAIN_FORMAT.commit())
            .replace(NEO4J_SHELL.schemaAwait(), PLAIN_FORMAT.schemaAwait());

    static final String EXPECTED_NEO4J_MERGE = EXPECTED_NODES_MERGE + EXPECTED_SCHEMA + EXPECTED_RELATIONSHIPS_MERGE + EXPECTED_CLEAN_UP;

    static final String EXPECTED_ONLY_SCHEMA_CYPHER_SHELL = EXPECTED_ONLY_SCHEMA_NEO4J_SHELL.replace(NEO4J_SHELL.begin(), CYPHER_SHELL.begin())
            .replace(NEO4J_SHELL.commit(), CYPHER_SHELL.commit()).replace(NEO4J_SHELL.schemaAwait(), CYPHER_SHELL.schemaAwait()) + EXPECTED_INDEXES_AWAIT;

}
