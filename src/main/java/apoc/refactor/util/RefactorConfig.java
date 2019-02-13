package apoc.refactor.util;

import java.util.Collections;
import java.util.Map;

import static apoc.util.Util.toBoolean;

/**
 * @author AgileLARUS
 *
 * @since 20-06-17
 */
public class RefactorConfig {

	public static final String COMBINE = "combine";
	public static final String DISCARD = "discard";
	public static final String OVERWRITE = "overwrite";
	public static final String OVERRIDE = "override";

	private static String MATCH_ALL = ".*";

	private Map<String,String> propertiesManagement = Collections.singletonMap(MATCH_ALL, OVERWRITE);

	private boolean mergeRelsAllowed;
	private boolean mergeVirtualRels;
	private boolean selfRel;
	private boolean countMerge;
	private boolean countProperties;
	private boolean hasProperties;
	private boolean collapsedLabel;

	public RefactorConfig(Map<String,Object> config) {
		Object value = config.get("properties");
		hasProperties = value != null;
		if (value instanceof String) {
			this.propertiesManagement = Collections.singletonMap(MATCH_ALL, value.toString());
		} else if (value instanceof Map) {
			this.propertiesManagement = (Map<String,String>)value;
		}

		this.mergeRelsAllowed = toBoolean(config.get("mergeRels"));
		this.mergeVirtualRels = toBoolean(config.getOrDefault("mergeRelsVirtual", true));
		this.selfRel = toBoolean(config.get("selfRel"));
		this.countMerge = toBoolean(config.getOrDefault("countMerge", true));
		this.countProperties = toBoolean(config.getOrDefault("countProperties", true));
		this.collapsedLabel = toBoolean(config.get("collapsedLabel"));
	}

	public String getMergeMode(String name){
		for (String key : propertiesManagement.keySet()) {
			if (!key.equals(MATCH_ALL) && name.matches(key)) {
				return propertiesManagement.get(key);
			}
		}
		return propertiesManagement.getOrDefault(name,propertiesManagement.getOrDefault(MATCH_ALL, OVERWRITE));

	}

	public String getMergeModeVirtual(String name){
		for (String key : propertiesManagement.keySet()) {
			if (!key.equals(MATCH_ALL) && name.matches(key)) {
				return propertiesManagement.get(key);
			}
		}
		return propertiesManagement.getOrDefault(name,propertiesManagement.getOrDefault(MATCH_ALL, DISCARD));

	}

	public boolean getMergeRelsAllowed(){
		return mergeRelsAllowed;
	}

	public boolean isSelfRel(){ return selfRel; }

	public boolean hasProperties() {
		return hasProperties;
	}

	public boolean isCountMerge() { return this.countMerge;	}

	public boolean isCollapsedLabel() {
		return collapsedLabel;
	}

	public boolean isCountProperties() {
		return countProperties;
	}

	public boolean isMergeVirtualRels() {
		return mergeVirtualRels;
	}
}
