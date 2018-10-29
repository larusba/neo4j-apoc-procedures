package apoc.export.json;

import apoc.export.util.ExportConfig;
import apoc.export.util.Format;
import apoc.export.util.Reporter;
import apoc.meta.Meta;
import apoc.result.ProgressInfo;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.util.MinimalPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.spatial.Point;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.time.temporal.TemporalAccessor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class JsonFormat implements Format {
    private final GraphDatabaseService db;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    static {
        SimpleModule module = new SimpleModule("Neo4jApocSerializer", new Version(1, 0, 0, ""));
        module.addSerializer(Point.class, new PointSerializer());
        module.addSerializer(TemporalAccessor.class, new TemporalSerializer());
        OBJECT_MAPPER.registerModule(module);
    }

    public JsonFormat(GraphDatabaseService db) {
        this.db = db;
    }

    @Override
    public ProgressInfo load(Reader reader, Reporter reporter, ExportConfig config) throws Exception {
        return null;
    }

    private ProgressInfo dump(Writer writer, Reporter reporter, ExportConfig config, Consumer<JsonGenerator> consumer) throws Exception {
        try (Transaction tx = db.beginTx()) {
            JsonGenerator jsonGenerator = getJsonGenerator(writer);

            consumer.accept(jsonGenerator);

            jsonGenerator.close();
            tx.success();
            writer.close();
            return reporter.getTotal();
        }
    }

    @Override
    public ProgressInfo dump(SubGraph graph, Writer writer, Reporter reporter, ExportConfig config) throws Exception {
        Consumer<JsonGenerator> consumer = (jsonGenerator) -> {
            try {
                writeNodes(graph.getNodes(), reporter, jsonGenerator, config);
                writeRels(graph.getRelationships(), reporter, jsonGenerator, config, true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        return dump(writer, reporter, config, consumer);
    }

    public ProgressInfo dump(Result result, Writer writer, Reporter reporter, ExportConfig config) throws Exception {
        Consumer<JsonGenerator> consumer = (jsonGenerator) -> {
            try {
                String[] header = result.columns().toArray(new String[result.columns().size()]);
                result.accept((row) -> {
                    writeJsonResult(reporter, header, jsonGenerator, row, config);
                    reporter.nextRow();
                    return true;
                });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
        return dump(writer, reporter, config, consumer);
    }

    private JsonGenerator getJsonGenerator(Writer writer) throws IOException {
        JsonFactory jsonF = new JsonFactory();
        JsonGenerator jsonGenerator = jsonF.createGenerator(writer);
        jsonGenerator.setCodec(OBJECT_MAPPER);
        jsonGenerator.setPrettyPrinter(new MinimalPrettyPrinter("\n"));
        return jsonGenerator;
    }

    private void writeNodes(Iterable<Node> nodes, Reporter reporter, JsonGenerator jsonGenerator,ExportConfig config) throws IOException {
        for (Node node : nodes) {
            writeNode(reporter, jsonGenerator, node, config);
        }
    }

    private void writeNode(Reporter reporter, JsonGenerator jsonGenerator, Node node, ExportConfig config) throws IOException {
        Map<String, Object> allProperties = node.getAllProperties();
        JsonFormatSerializer.DEFAULT.writeNode(jsonGenerator, node, config);
        reporter.update(1, 0, allProperties.size());
    }

    private void writeRels(Iterable<Relationship> rels, Reporter reporter, JsonGenerator jsonGenerator, ExportConfig config, boolean writeNodeProperties) throws IOException {
        for (Relationship rel : rels) {
            writeRel(reporter, jsonGenerator, rel, config, writeNodeProperties);
        }
    }

    private void writeRel(Reporter reporter, JsonGenerator jsonGenerator, Relationship rel, ExportConfig config, boolean writeNodeProperties) throws IOException {
        Map<String, Object> allProperties = rel.getAllProperties();
        JsonFormatSerializer.DEFAULT.writeRelationship(jsonGenerator, rel, writeNodeProperties, config);
        reporter.update(0, 1, allProperties.size());
    }

    private void writeJsonResult(Reporter reporter, String[] header, JsonGenerator jsonGenerator, Result.ResultRow row, ExportConfig config) throws IOException {
        jsonGenerator.writeStartObject();
        for (int col = 0; col < header.length; col++) {
            String keyName = header[col];
            Object value = row.get(keyName);
            write(reporter, jsonGenerator, config, keyName, value, true);
        }
        jsonGenerator.writeEndObject();
    }

    private void write(Reporter reporter, JsonGenerator jsonGenerator, ExportConfig config, String keyName, Object value, boolean writeKey) throws IOException {
        Meta.Types type = Meta.Types.of(value);
        switch (type) {
            case NODE:
                writeFieldName(jsonGenerator, keyName, writeKey);
                writeNode(reporter, jsonGenerator, (Node) value, config);
                break;
            case RELATIONSHIP:
                writeFieldName(jsonGenerator, keyName, writeKey);
                writeRel(reporter, jsonGenerator, (Relationship) value, config, false);
                break;
            case PATH:
                writeFieldName(jsonGenerator, keyName, writeKey);
                writePath(reporter, jsonGenerator, config, (Path) value);
                break;
            case MAP:
                if (writeKey) {
                    jsonGenerator.writeObjectFieldStart(keyName);
                } else {
                    jsonGenerator.writeStartObject();
                    writeKey = true;
                }
                Map<String, Object> map = (HashMap<String, Object>) value;
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    write(reporter, jsonGenerator, config, entry.getKey(), entry.getValue(), writeKey);
                }
                jsonGenerator.writeEndObject();
                break;
            case LIST:
                if (writeKey) {
                    jsonGenerator.writeArrayFieldStart(keyName);
                } else {
                    jsonGenerator.writeStartArray();
                }
                List<Object> list = (List<Object>) value;
                for (Object elem : list) {
                    write(reporter, jsonGenerator, config, keyName, elem, false);
                }
                jsonGenerator.writeEndArray();
                break;
            default:
                JsonFormatSerializer.DEFAULT.serializeProperty(jsonGenerator, keyName, value, writeKey);
                reporter.update(0, 0, 1);
                break;

        }
    }

    private void writeFieldName(JsonGenerator jsonGenerator, String keyName, boolean writeKey) throws IOException {
        if (writeKey) {
            jsonGenerator.writeFieldName(keyName);
        }
    }

    private void writePath(Reporter reporter, JsonGenerator jsonGenerator, ExportConfig config, Path path) throws IOException {
        jsonGenerator.writeStartArray();
        writeRels(path.relationships(), reporter, jsonGenerator, config, true);
        jsonGenerator.writeEndArray();
    }

}