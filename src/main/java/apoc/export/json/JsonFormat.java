package apoc.export.json;

import apoc.export.util.ExportConfig;
import apoc.export.util.Format;
import apoc.export.util.Reporter;
import apoc.result.ProgressInfo;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class JsonFormat implements Format {
    private final GraphDatabaseService db;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

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

            JsonFactory jsonF = new JsonFactory();
            JsonGenerator jsonGenerator = jsonF.createGenerator(writer);
            jsonGenerator.setCodec(OBJECT_MAPPER);
            jsonGenerator.setPrettyPrinter(new MinimalPrettyPrinter("\n"));
            writeNodes(graph, reporter, jsonGenerator, config);
            writeRels(graph, reporter, jsonGenerator, config);
            jsonGenerator.close();
            tx.success();
            writer.close();

            return reporter.getTotal();

        }
    }

    private void writeNodes(SubGraph graph, Reporter reporter, JsonGenerator jsonGenerator, ExportConfig config) throws IOException {
        for (Node node : graph.getNodes()) {

            writeNode(reporter, jsonGenerator, node, config);
        }
    }

    private void writeNode(Reporter reporter, JsonGenerator jsonGenerator, Node node, ExportConfig config) throws IOException {
        Map<String, Object> allProperties = node.getAllProperties();

        JsonFormatSerializer.DEFAULT.writeNode(jsonGenerator, node.getId(), node.getLabels(), allProperties, config);
        reporter.update(1, 0, allProperties.size());
    }

    private void writeRels(SubGraph graph, Reporter reporter, JsonGenerator jsonGenerator, ExportConfig config) throws IOException {
        for (Relationship rel : graph.getRelationships()) {

            writeRel(reporter, jsonGenerator, rel, config);
        }
    }

    private void writeRel(Reporter reporter, JsonGenerator jsonGenerator, Relationship rel, ExportConfig config) throws IOException {
        Map<String, Object> allProperties = rel.getAllProperties();
        JsonFormatSerializer.DEFAULT.writeRelationship(jsonGenerator, rel.getId(), rel.getStartNodeId(), rel.getEndNodeId(), rel.getType().name(), allProperties, config);
        reporter.update(0, 1, allProperties.size());
    }

    public ProgressInfo dump(Result result, Writer writer, Reporter reporter, ExportConfig config) throws Exception {
        try (Transaction tx = db.beginTx()) {

            String[] header = result.columns().toArray(new String[result.columns().size()]);

            JsonFactory jsonF = new JsonFactory();
            JsonGenerator jsonGenerator = jsonF.createGenerator(writer);
            jsonGenerator.setPrettyPrinter(new MinimalPrettyPrinter("\n"));
            jsonGenerator.setCodec(OBJECT_MAPPER);

            result.accept((row) -> {
                writeJsonResult(reporter, header, jsonGenerator, row, config);
                reporter.nextRow();
                return true;
            });

            jsonGenerator.close();
            tx.success();
            writer.close();
            return reporter.getTotal();
        }
    }

    private void writeJsonResult(Reporter reporter, String[] header, JsonGenerator jsonGenerator, Result.ResultRow row, ExportConfig config) throws IOException {

        boolean isMap = Stream.of(header).allMatch(col ->{
            Object value = row.get(col);
            return !(value instanceof Node) && !(value instanceof Relationship) && !(value instanceof Path);
        });

        if(isMap){
            jsonGenerator.writeStartObject();
            jsonGenerator.writeFieldName("data");
            jsonGenerator.writeStartObject();
            for (int col = 0; col < header.length; col++) {
                String keyName = header[col];
                Object value = row.get(keyName);
                JsonFormatSerializer.DEFAULT.serializeProperty(jsonGenerator, keyName, value);
                checkWriteDataTypes(jsonGenerator,config,keyName,value);
                reporter.update(0, 0, 1);
            }
            jsonGenerator.writeEndObject();
            jsonGenerator.writeEndObject();
        }else {
            jsonGenerator.writeStartObject();
            for (int col = 0; col < header.length; col++) {
                String keyName = header[col];
                Object value = row.get(keyName);

                if (value == null) {
                    jsonGenerator.writeNullField(keyName);
                } else if (value instanceof Node) {
                    jsonGenerator.writeFieldName(keyName);
                    writeNode(reporter, jsonGenerator, (Node) value, config);
                } else if (value instanceof Relationship) {
                    jsonGenerator.writeFieldName(keyName);
                    writeRel(reporter, jsonGenerator, (Relationship) value, config);
                } else if (value instanceof Path) {
                    jsonGenerator.writeFieldName(keyName);
                    jsonGenerator.writeStartObject();
                    jsonGenerator.writeFieldName("startNode");
                    Path path = (Path) value;
                    writeNode(reporter, jsonGenerator, path.startNode(), config);
                    jsonGenerator.writeFieldName("relationship");
                    for (Relationship rel : path.relationships()) {
                        writeRel(reporter, jsonGenerator, rel, config);
                    }
                    jsonGenerator.writeFieldName("endNode");
                    writeNode(reporter, jsonGenerator, path.endNode(), config);
                    jsonGenerator.writeEndObject();
                } else if (!(value instanceof PropertyContainer)) {
                    JsonFormatSerializer.DEFAULT.serializeProperty(jsonGenerator, keyName, value);
                    reporter.update(0, 0, 1);
                }
            }
            jsonGenerator.writeEndObject();
        }

    }

    private void checkWriteDataTypes(JsonGenerator jsonGenerator, ExportConfig config, String keyName, Object value) throws IOException {
        if(config.useTypes() && value !=null) {
            Map<String, Object> m = new HashMap<>();
            m.put(keyName, value);
            JsonFormatSerializer.DEFAULT.writeDataTypes(jsonGenerator, m);
        }
    }

}