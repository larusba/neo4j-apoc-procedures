package apoc.meta;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class MetaConfig {

    Set<String> includesLabels;
    Set<String> includesRels;
    Set<String> excludes;
    Map<String, Long> sample;
    long maxRels;

    public MetaConfig(Map<String,Object> config) {
        config = config != null ? config : Collections.emptyMap();
        this.includesLabels = new HashSet<>((Collection<String>)config.getOrDefault("labels",Collections.EMPTY_SET));
        this.includesRels = new HashSet<>((Collection<String>)config.getOrDefault("rels",Collections.EMPTY_SET));
        this.excludes = new HashSet<>((Collection<String>)config.getOrDefault("excludes",Collections.EMPTY_SET));
        this.sample = (Map<String, Long>) config.getOrDefault("sample", Collections.emptyMap());
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

    public Map<String, Long> getSample() {
        return sample;
    }

    public void setSample(Map<String, Long> sample) {
        this.sample = sample;
    }

    public long getMaxRels() {
        return maxRels;
    }

    public void setMaxRels(long maxRels) {
        this.maxRels = maxRels;
    }

    /*
        This method manage sample config. User can set sample for each label
        if sample config is not set all nodes are considered.
        Return the sample with a randomize +/-10% of the value set from user
     */
    public long getSampleForLabel(String label) {

        long sampleLabel = sample.getOrDefault(label, -1L);

        if(sampleLabel != -1L) {
            long min = (long) Math.floor(sampleLabel - (sampleLabel * 0.1D));
            long max = (long) Math.ceil(sampleLabel + (sampleLabel * 0.1D));
            return ThreadLocalRandom.current().nextLong(min, max);
        } else {
            return sampleLabel;
        }
    }
}
