package apoc.export.json;

import apoc.export.json.serialize.CustomSerializeNode;
import apoc.export.json.serialize.CustomSerializeRelationship;
import apoc.export.util.ExportConfig;
import apoc.export.util.Format;
import apoc.export.util.Reporter;
import apoc.result.ProgressInfo;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;

import static apoc.util.JsonUtil.getPropertiesArray;

public class JsonFormat implements Format {
    private final GraphDatabaseService db;

    public JsonFormat(GraphDatabaseService db) {
        this.db = db;
    }

    @Override
    public ProgressInfo load(Reader reader, Reporter reporter, ExportConfig config) throws Exception {
        return null;
    }

    @Override
    public ProgressInfo dump(SubGraph graph, Writer writer, Reporter reporter, ExportConfig config) throws Exception {
        try (Transaction tx = db.beginTx()) {
            writeAll(graph, reporter, config, writer);
            tx.success();
            writer.close();
            return reporter.getTotal();
        }
    }

    public ProgressInfo dump(Result result, Writer writer, Reporter reporter, ExportConfig config) throws Exception {
        try (Transaction tx = db.beginTx()) {
            ObjectMapper mapperNode = new ObjectMapper();
            ObjectMapper mapperRel = new ObjectMapper();
            ObjectMapper mapperProp = new ObjectMapper();

            List<String> column = result.columns();
            String[] header = column.toArray(new String[result.columns().size()]);

            SimpleModule module = new SimpleModule("CustomSerializeNode", new Version(1, 0, 0, null, null, null));
            module.addSerializer(Node.class, new CustomSerializeNode());

            SimpleModule moduleRel = new SimpleModule("CustomSerializeRelationship", new Version(1, 0, 0, null, null, null));
            moduleRel.addSerializer(Relationship.class, new CustomSerializeRelationship());

            mapperNode.registerModule(module);
            mapperRel.registerModule(moduleRel);

            writer.write("[ ");
            result.accept((row) -> {
                ObjectNode objectNode = mapperProp.createObjectNode();
                String node = "", rel = "", property = "";
                for (int col = 0; col < header.length; col++) {
                    Object value = row.get(header[col]);
                    if (value instanceof Node) {
                        node = mapperNode.writerWithDefaultPrettyPrinter().writeValueAsString(value);
                    }
                    if (value instanceof Relationship) {
                        rel = mapperRel.writerWithDefaultPrettyPrinter().writeValueAsString(value);
                    }
                    if (!(value instanceof PropertyContainer)) {
                        if(value != null) {
                            property = getPropertiesArray(mapperProp, header[col], value, objectNode);
                        }
                    }
                    reporter.update(value instanceof Node ? 1 : 0, value instanceof Relationship ? 1 : 0, value instanceof PropertyContainer ? 0 : 1);
                }
                writeJsonToFile(writer, property, result.hasNext());
                writeJsonToFile(writer, rel, result.hasNext());
                writeJsonToFile(writer, node, result.hasNext());

                reporter.nextRow();
                return true;
            });
            writer.write(" ]");
            tx.success();
            writer.close();
            return reporter.getTotal();
        }
    }

    private void writeAll(SubGraph graph, Reporter reporter, ExportConfig config, Writer writer) throws IOException {

        ObjectMapper mapperNode = new ObjectMapper();
        ObjectMapper mapperRel = new ObjectMapper();

        SimpleModule module = new SimpleModule("CustomSerializeNode", new Version(1, 0, 0, null, null, null));
        module.addSerializer(Node.class, new CustomSerializeNode());

        SimpleModule moduleRel = new SimpleModule("CustomSerializeRelationship", new Version(1, 0, 0, null, null, null));
        moduleRel.addSerializer(Relationship.class, new CustomSerializeRelationship());

        mapperNode.registerModule(module);
        mapperRel.registerModule(moduleRel);

        writer.write("[ ");
        writeNodes(graph, mapperNode, reporter, writer);
        writeReels(graph, mapperRel, reporter, writer);
        writer.write(" ]");
    }

    private void writeNodes(SubGraph graph, ObjectMapper mapper, Reporter reporter, Writer writer) throws IOException {
        for (Node node : graph.getNodes()) {
            reporter.update(1, 0, node.getAllProperties().size());
            String nodeResult = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
            writeJsonToFile(writer, nodeResult, true);
        }
    }

    private void writeReels(SubGraph graph, ObjectMapper mapper, Reporter reporter, Writer writer) throws IOException {
        for (Iterator<Relationship> iter = graph.getRelationships().iterator(); iter.hasNext(); ) {
            Relationship rel = iter.next();
            reporter.update(0, 1, 0);
            String relResult = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rel);
            writeJsonToFile(writer, relResult, iter.hasNext());
        }
    }

    private void writeJsonToFile(Writer writer, String text, boolean end) throws IOException {
        if(!text.equals(""))
            writer.write(text + (!end ? "" : ", "));
    }

}