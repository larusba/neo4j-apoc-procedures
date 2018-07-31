package apoc.export.json.serialize;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;

import static apoc.export.json.serialize.SerializerUtil.serializerProperty;

public class NodeSerializer extends StdSerializer<Node> {

    public NodeSerializer() {
        this(null);
    }

    public NodeSerializer(Class<Node> t) {
        super(t);
    }

    @Override
    public void serialize(Node node, JsonGenerator jsonGenerator, SerializerProvider serializer) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeNumberField("id", node.getId());

        jsonGenerator.writeArrayFieldStart("labels");
        for (Label arg: node.getLabels()) {
            jsonGenerator.writeString(arg.toString());
        }
        jsonGenerator.writeEndArray();
        serializerProperty(jsonGenerator, node);
        jsonGenerator.writeArrayFieldStart("relationships");
        for (Relationship rel: node.getRelationships()) {
            jsonGenerator.writeString(rel.toString());
        }
        jsonGenerator.writeEndArray();

        jsonGenerator.writeEndObject();
    }
}
