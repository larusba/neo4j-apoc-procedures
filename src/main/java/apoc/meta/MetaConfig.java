package apoc.meta;

import java.util.*;

public class MetaConfig {

    Set<String> includesLabels;
    Set<String> includesRels;
    Set<String> excludes;
    long sample;
    long maxRels;

    public MetaConfig(Map<String,Object> config) {
        config = config != null ? config : Collections.emptyMap();
        this.includesLabels = new HashSet<>((Collection<String>)config.getOrDefault("labels",Collections.EMPTY_SET));
        this.includesRels = new HashSet<>((Collection<String>)config.getOrDefault("rels",Collections.EMPTY_SET));
        this.excludes = new HashSet<>((Collection<String>)config.getOrDefault("excludes",Collections.EMPTY_SET));
        this.sample = (long) config.getOrDefault("sample", -1L);
        this.maxRels = (long) config.getOrDefault("maxRels", -1L);
    }

    public Set<String> getIncludesLabels() {
        return includesLabels;
    }

    public void setIncludesLabels(Set<String> includesLabels) {
        this.includesLabels = includesLabels;
    }

    public Set<String> getIncludesRels() {
        return includesRels;
    }

    public void setIncludesRels(Set<String> includesRels) {
        this.includesRels = includesRels;
    }

    public Set<String> getExcludes() {
        return excludes;
    }

    public void setExcludes(Set<String> excludes) {
        this.excludes = excludes;
    }

    public long getSample() {
        return sample;
    }

    public void setSample(long sample) {
        this.sample = sample;
    }

    public long getMaxRels() {
        return maxRels;
    }

    public void setMaxRels(long maxRels) {
        this.maxRels = maxRels;
    }
}
