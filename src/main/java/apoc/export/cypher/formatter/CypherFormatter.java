package apoc.export.cypher.formatter;

import apoc.export.util.ExportConfig;
import apoc.export.util.Reporter;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;

/**
 * @author AgileLARUS
 *
 * @since 16-06-2017
 */
public interface CypherFormatter {

	String statementForNode(Node node, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames);

	String statementForRelationship(Relationship relationship,  Map<String, String> uniqueConstraints, Set<String> indexedProperties);

	String statementForIndex(String label, Iterable<String> keys);

	String statementForConstraint(String label, String key);

	String statementForCleanUp(int batchSize);

	void statementForSameNodes(Iterable<Node> node, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames, ExportConfig exportConfig, PrintWriter out, Reporter reporter);

	void statementForSameRelationship(Iterable<Relationship> relationship, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames, ExportConfig exportConfig, PrintWriter out, Reporter reporter);

}
