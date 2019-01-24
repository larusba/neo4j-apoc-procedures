package apoc.convert;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author AgileLARUS
 *
 * @since 28-01-2019
 */
public class ConvertConfig {

    private Map<String, List<String>> nodes;
    private Map<String, List<String>> relationships;

    public ConvertConfig(Map<String,Object> config) {

        this.nodes = (Map<String, List<String>>) config.getOrDefault("nodes", Collections.EMPTY_MAP);
        this.relationships = (Map<String, List<String>>) config.getOrDefault("relationships", Collections.EMPTY_MAP);

        this.nodes.values().forEach(s -> validateListProperties(s));
        this.relationships.values().forEach(s -> validateListProperties(s));
    }

    public Map<String, List<String>> getNodes() {
        return nodes;
    }

    public Map<String, List<String>> getRelationships() {
        return relationships;
    }

    private void validateListProperties(List<String> list) {
        if (list == null || list.isEmpty()) throw new RuntimeException("List can't be empty!");
        boolean isFirstExclude = list.get(0).startsWith("-");
        Optional<String> hasMixedProp = list.stream().skip(1).filter(prop ->
                (isFirstExclude && !prop.startsWith("-")) || (!isFirstExclude && prop.startsWith("-"))).findFirst();
        if (hasMixedProp.isPresent()) {
            throw new RuntimeException("Only include or exclude attribute are possible!");
        }
    }
}
