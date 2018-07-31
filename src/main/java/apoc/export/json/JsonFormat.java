package apoc.export.json;

import apoc.export.json.serialize.NodeSerializer;
import apoc.export.json.serialize.RelationshipSerializer;
import apoc.export.util.ExportConfig;
import apoc.export.util.Format;
import apoc.export.util.Reporter;
import apoc.result.ProgressInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Iterator;

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
            ObjectMapper mapperNode = JsonCustomMappers.createMapperForNodes();
            ObjectMapper mapperRel = JsonCustomMappers.createMapperForRels();

            writeJsonToFile(writer,"[ ", false);
            writeNodes(graph, mapperNode, reporter, writer);
            writeReels(graph, mapperRel, reporter, writer);
            writeJsonToFile(writer," ]", false);

            tx.success();
            writer.close();
            return reporter.getTotal();
        }
    }

    private void writeNodes(SubGraph graph, ObjectMapper mapper, Reporter reporter, Writer writer) throws IOException {
        for (Node node : graph.getNodes()) {
            reporter.update(1, 0, node.getAllProperties().size());
            writeJsonToFile(writer, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node), true);
        }
    }

    private void writeReels(SubGraph graph, ObjectMapper mapper, Reporter reporter, Writer writer) throws IOException {
        Iterator<Relationship> relationships = graph.getRelationships().iterator();
        relationships.forEachRemaining((rels)->{
            try {
                reporter.update(0, 1, 0);
                writeJsonToFile(writer, mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rels), relationships.hasNext());
            } catch (JsonProcessingException e) { }
            catch (IOException e) { }
        });
    }

    public ProgressInfo dump(Result result, Writer writer, Reporter reporter, ExportConfig config) throws Exception {
        try (Transaction tx = db.beginTx()) {

            String[] header = result.columns().toArray(new String[result.columns().size()]);

            ObjectMapper mapperNode = JsonCustomMappers.createMapperForNodes();
            ObjectMapper mapperRel = JsonCustomMappers.createMapperForRels();
            ObjectMapper mapperProp = new ObjectMapper();

            writeJsonToFile(writer,"[ ", false);
            result.accept((row) -> {
                writeJsonResult(result, writer, reporter, header, mapperNode, mapperRel, mapperProp, row);
                reporter.nextRow();
                return true;
            });
            writeJsonToFile(writer," ]", false);
            tx.success();
            writer.close();
            return reporter.getTotal();
        }
    }

    private void writeJsonResult(Result result, Writer writer, Reporter reporter, String[] header, ObjectMapper mapperNode, ObjectMapper mapperRel, ObjectMapper mapperProp, Result.ResultRow row) throws IOException {
        ObjectNode objectNode = mapperProp.createObjectNode();
        String node = "", rel = "", property = "";
        for (int col = 0; col < header.length; col++) {
            Object value = row.get(header[col]);
            if (value instanceof Node) {
                node = mapperNode.writerWithDefaultPrettyPrinter().writeValueAsString(value);
                writeJsonToFile(writer, node, result.hasNext() ? result.hasNext() : col+1 == header.length ? false : true);
            }
            if (value instanceof Relationship) {
                rel = mapperRel.writerWithDefaultPrettyPrinter().writeValueAsString(value);
                writeJsonToFile(writer, rel, result.hasNext() ? result.hasNext() : col+1 == header.length ? false : true);
            }
            if (!(value instanceof PropertyContainer)) {
                if(value != null) {
                    property = getPropertiesArray(mapperProp, header[col], value, objectNode);
                }
            }
            reporter.update(value instanceof Node ? 1 : 0, value instanceof Relationship ? 1 : 0, value instanceof PropertyContainer ? 0 : 1);
        }
        writeJsonToFile(writer, property, result.hasNext());
    }

    private static String getPropertiesArray(ObjectMapper mapperProp, String key, Object value, ObjectNode objectNode) throws JsonProcessingException {
        if(!value.getClass().isArray()) {
            objectNode.put(key, value.toString());
        }else {
            String[] prop;
            ArrayNode arrayPropertiesInside = mapperProp.createArrayNode();
            for(String property : prop = (String[]) value){
                arrayPropertiesInside.add(property);
            }
            objectNode.putPOJO(key, arrayPropertiesInside);
        }

        return mapperProp.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
    }

    private void writeJsonToFile(Writer writer, String text, boolean end) throws IOException {
        if(!text.equals(""))
            writer.write(text + (!end ? "" : ", "));
    }

    private static class JsonCustomMappers {

        private static ObjectMapper createMapperForNodes(){
            ObjectMapper mapperNode = new ObjectMapper();
            SimpleModule module = new SimpleModule("CustomSerializeNode", new Version(1, 0, 0, null, null, null));
            module.addSerializer(Node.class, new NodeSerializer());
            mapperNode.registerModule(module);

            return mapperNode;
        }

        private static ObjectMapper createMapperForRels(){
            ObjectMapper mapperRels = new ObjectMapper();
            SimpleModule moduleRel = new SimpleModule("CustomSerializeRelationship", new Version(1, 0, 0, null, null, null));
            moduleRel.addSerializer(Relationship.class, new RelationshipSerializer());
            mapperRels.registerModule(moduleRel);

            return mapperRels;
        }
    }


}