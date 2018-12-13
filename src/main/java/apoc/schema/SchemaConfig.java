package apoc.schema;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * @author ab-larus
 * @since 17.12.18
 */
public class SchemaConfig {

    private Collection<String> labels;
    private Collection<String> excludeLabels;
    private Collection<String> relationships;
    private Collection<String> excludeRelationships;

    public Collection<String> getLabels() {
        return labels;
    }

    public Collection<String> getExcludeLabels() {
        return excludeLabels;
    }

    public Collection<String> getRelationships() {
        return relationships;
    }

    public Collection<String> getExcludeRelationships() {
        return excludeRelationships;
    }

    public SchemaConfig(Map<String,Object> config) {
        config = config != null ? config : Collections.emptyMap();
        this.labels = (Collection<String>) config.getOrDefault("labels", Collections.EMPTY_LIST);
        this.excludeLabels = (Collection<String>) config.getOrDefault("excludeLabels", Collections.EMPTY_LIST);
        validateParameters(this.labels, this.excludeLabels, "labels");
        this.relationships = (Collection<String>) config.getOrDefault("relationships", Collections.EMPTY_LIST);
        this.excludeRelationships = (Collection<String>) config.getOrDefault("excludeRelationships", Collections.EMPTY_LIST);
        validateParameters(this.labels, this.excludeLabels, "relationships");
    }

    private void validateParameters(Collection<String> include, Collection<String> exclude, String parametrType){
        if(include != Collections.EMPTY_LIST && exclude != Collections.EMPTY_LIST)
            throw new IllegalArgumentException(String.format("Parameters %s and exclude%s are both valuated. Please check parameters and valuate only one.", parametrType, parametrType));
    }
}
