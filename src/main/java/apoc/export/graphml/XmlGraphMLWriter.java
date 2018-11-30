package apoc.export.graphml;

import apoc.export.util.*;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import static apoc.export.util.MetaInformation.getLabelsString;
import static apoc.export.util.MetaInformation.updateKeyTypes;

/**
 * @author mh
 * @since 21.01.14
 */
public class XmlGraphMLWriter {

    public void write(SubGraph graph, Writer writer, Reporter reporter, ExportConfig config) throws Exception {
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        XMLStreamWriter xmlWriter = xmlOutputFactory.createXMLStreamWriter(writer);
        writeHeader(xmlWriter);
        writeKey(xmlWriter, graph, config.useTypes());
        writeGraph(xmlWriter);
        for (Node node : graph.getNodes()) {
            int props = writeNode(xmlWriter, node, config);
            reporter.update(1, 0, props);
        }
        for (Relationship rel : graph.getRelationships()) {
            int props = writeRelationship(xmlWriter, rel);
            reporter.update(0, 1, props);
        }
        writeFooter(xmlWriter);
    }

    private void writeKey(XMLStreamWriter writer, SubGraph ops, boolean useTypes) throws Exception {
        Map<String, Class> keyTypes = new HashMap<>();
        for (Node node : ops.getNodes()) {
            if (node.getLabels().iterator().hasNext()) {
                keyTypes.put("label", String.class);
            }
            updateKeyTypes(keyTypes, node);
        }
        keyTypes.put("TYPE", String.class);
        writeKey(writer, keyTypes, "node", useTypes);
        keyTypes.clear();
        for (Relationship rel : ops.getRelationships()) {
            keyTypes.put("label", String.class);
            updateKeyTypes(keyTypes, rel);
        }
        keyTypes.put("TYPE", String.class);
        writeKey(writer, keyTypes, "edge", useTypes);
    }

    private void writeKey(XMLStreamWriter writer, Map<String, Class> keyTypes, String forType, boolean useTypes) throws IOException, XMLStreamException {
        for (Map.Entry<String, Class> entry : keyTypes.entrySet()) {
            Class typeClass = entry.getValue();
            String type = MetaInformation.typeFor(typeClass, MetaInformation.GRAPHML_ALLOWED);
            if (type == null) continue;
            writer.writeEmptyElement("key");
            writer.writeAttribute("id", entry.getKey());
            writer.writeAttribute("for", forType);
            writer.writeAttribute("attr.name", entry.getKey());
            if (useTypes) {
                if (typeClass.isArray()) {
                    writer.writeAttribute("attr.type", "string");
                    writer.writeAttribute("attr.list", type);
                } else {
                    writer.writeAttribute("attr.type", type);
                }
            }
            newLine(writer);
        }
    }

    private int writeNode(XMLStreamWriter writer, Node node, ExportConfig config) throws IOException, XMLStreamException {
        writer.writeStartElement("node");
        writer.writeAttribute("id", id(node));
        writeLabels(writer, node, config);
        writeLabelsAsData(writer, node, config);
        int props = writeProps(writer, node);
        endElement(writer);
        return props;
    }

    private String id(Node node) {
        return "n" + node.getId();
    }

    private void writeLabels(XMLStreamWriter writer, Node node, ExportConfig config) throws IOException, XMLStreamException {
        String labelsString = getLabelsString(config, node);
        if (!labelsString.isEmpty()) writer.writeAttribute("labels", labelsString);
    }

    private void writeLabelsAsData(XMLStreamWriter writer, Node node, ExportConfig config) throws IOException, XMLStreamException {
        String labelsString = getLabelsString(config, node);
        if (labelsString.isEmpty()) return;
        String delimiter = ":";
        writeData(writer, "TYPE", delimiter + FormatUtils.joinLabels(node, delimiter));
        writeData(writer, ExportFormat.GEPHI == config.getFormat() ? "label" : "labels", labelsString);
    }

    private int writeRelationship(XMLStreamWriter writer, Relationship rel) throws IOException, XMLStreamException {
        writer.writeStartElement("edge");
        writer.writeAttribute("id", id(rel));
        writer.writeAttribute("source", id(rel.getStartNode()));
        writer.writeAttribute("target", id(rel.getEndNode()));
        writer.writeAttribute("label", rel.getType().name());
        writeData(writer, "label", rel.getType().name());
        writeData(writer, "TYPE", rel.getType().name());
        int props = writeProps(writer, rel);
        endElement(writer);
        return props;
    }

    private String id(Relationship rel) {
        return "e" + rel.getId();
    }

    private void endElement(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeEndElement();
        newLine(writer);
    }

    private int writeProps(XMLStreamWriter writer, PropertyContainer node) throws IOException, XMLStreamException {
        int count = 0;
        for (String prop : node.getPropertyKeys()) {
            Object value = node.getProperty(prop);
            writeData(writer, prop, value);
            count++;
        }
        return count;
    }

    private void writeData(XMLStreamWriter writer, String prop, Object value) throws IOException, XMLStreamException {
        writer.writeStartElement("data");
        writer.writeAttribute("key", prop);
        if (value != null) {
            writer.writeCharacters(FormatUtils.toString(value));
        }
        writer.writeEndElement();
    }

    private void writeFooter(XMLStreamWriter writer) throws IOException, XMLStreamException {
        endElement(writer);
        endElement(writer);
        writer.writeEndDocument();
    }

    private void writeHeader(XMLStreamWriter writer) throws IOException, XMLStreamException {
        writer.writeStartDocument("UTF-8", "1.0");
        newLine(writer);
        writer.writeStartElement("graphml"); // todo properties
        writer.writeNamespace("xmlns", "http://graphml.graphdrawing.org/xmlns");
        writer.writeAttribute("xmlns", "http://graphml.graphdrawing.org/xmlns", "xsi", "http://www.w3.org/2001/XMLSchema-instance");
        writer.writeAttribute("xsi", "", "schemaLocation", "http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd");
        newLine(writer);
    }

    private void writeGraph(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeStartElement("graph");
        writer.writeAttribute("id", "G");
        writer.writeAttribute("edgedefault", "directed");
        newLine(writer);
    }

    private void newLine(XMLStreamWriter writer) throws XMLStreamException {
        writer.writeCharacters(System.getProperty("line.separator"));
    }
}
