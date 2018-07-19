package apoc.export.json.serialize;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;

public class CustomSerializeNode extends StdSerializer<Node> {

    public CustomSerializeNode() {
        this(null);
    }

    public CustomSerializeNode(Class<Node> t) {
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

        jsonGenerator.writeObjectFieldStart("properties");
        node.getAllProperties().forEach((key, value)-> {
            try {
                if(value.getClass().isArray()){
                    String[] prop;
                    jsonGenerator.writeArrayFieldStart(key);
                    for(String property : prop = (String[]) value){
                        jsonGenerator.writeString(property);
                    }
                    jsonGenerator.writeEndArray();
                } else {
                    jsonGenerator.writeStringField(key, value.toString());
                }
            } catch (IOException e) {}
        });
        jsonGenerator.writeEndObject();

        jsonGenerator.writeArrayFieldStart("relationships");
        for (Relationship arg: node.getRelationships()) {
            jsonGenerator.writeString(arg.toString());
        }
        jsonGenerator.writeEndArray();

        jsonGenerator.writeEndObject();
    }
}
