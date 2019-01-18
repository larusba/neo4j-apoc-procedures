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
public class UpdateAllCypherFormatter extends AbstractCypherFormatter implements CypherFormatter {

	@Override
	public String statementForNode(Node node, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames) {
	    return super.mergeStatementForNode(CypherFormat.UPDATE_ALL, node, uniqueConstraints, indexedProperties, indexNames);
	}

	@Override
	public String statementForRelationship(Relationship relationship,  Map<String, String> uniqueConstraints, Set<String> indexedProperties) {
        return super.mergeStatementForRelationship(CypherFormat.UPDATE_ALL, relationship, uniqueConstraints, indexedProperties);
	}

	@Override
	public void statementForSameNodes(Iterable<Node> node, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames, ExportConfig exportConfig, PrintWriter out, Reporter reporter) {
	}

	@Override
	public void statementForSameRelationship(Iterable<Relationship> relationship, Map<String, String> uniqueConstraints, Set<String> indexedProperties, Set<String> indexNames, ExportConfig exportConfig, PrintWriter out, Reporter reporter) {
	}
}
