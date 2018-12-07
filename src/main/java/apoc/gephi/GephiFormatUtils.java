package apoc.gephi;

import apoc.export.util.FormatUtils;
import org.neo4j.graphdb.Node;

public class GephiFormatUtils {

    public static String captionGraphml(Node n, String[] captions) {
        for (String caption : captions) {
            if (n.hasProperty(caption)) return n.getProperty(caption).toString();
            for (String key : n.getPropertyKeys()) {
                if (key.toLowerCase().equalsIgnoreCase(caption)) return n.getProperty(key).toString();
            }
        }
        String delimiter = ":";
        return n.getLabels().iterator().hasNext() ? delimiter + FormatUtils.joinLabels(n, delimiter) : String.valueOf(n.getId());
    }
}
