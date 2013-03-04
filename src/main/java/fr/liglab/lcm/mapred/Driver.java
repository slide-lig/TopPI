package fr.liglab.lcm.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * Driver methods for our 3 map-reduce jobs
 */
public class Driver extends Configured implements Tool {
	public static final String KEY_INPUT    = "fr.liglab.lcm.input";
	public static final String KEY_OUTPUT   = "fr.liglab.lcm.output";
	public static final String KEY_MINSUP   = "fr.liglab.lcm.minsup";
	public static final String KEY_NBGROUPS = "fr.liglab.lcm.nbGroups";
	public static final String KEY_DO_TOP_K = "fr.liglab.lcm.topK";
	
	/**
	 * property key for item-counter-n-grouper output path
	 */
	static final String KEY_GROUPS_MAP = "fr.liglab.lcm.groupsMap";
	
	/**
	 * property key for mining output path
	 */
	static final String KEY_RAW_PATTERNS = "fr.liglab.lcm.rawpatterns";
	
	/**
	 * property key for aggregated patterns' output path
	 */
	static final String KEY_AGGREGATED_PATTERNS = "fr.liglab.lcm.aggregated";
	
	
	protected final Configuration conf;
	protected final String input;
	protected final String output;
	
	/**
	 * All public KEY_* are expected in the provided configuration
	 * (except KEY_DO_TOP_K : if it's not set, all patterns will be mined)
	 */
	public Driver(Configuration configuration) {
		this.conf = configuration;
		
		this.input = this.conf.get(KEY_INPUT);
		this.output = this.conf.get(KEY_OUTPUT);
		
		this.conf.setStrings(KEY_GROUPS_MAP, this.output + "/" + DistCache.GROUPSMAP_DIRNAME);
		this.conf.setStrings(KEY_RAW_PATTERNS, this.output + "/" + "rawMinedPatterns");
		this.conf.setStrings(KEY_AGGREGATED_PATTERNS, output + "/" + "patterns");
	}
	
	@Override
	public String toString() {
		int g = this.conf.getInt(KEY_NBGROUPS, -1);
		int k = this.conf.getInt(KEY_DO_TOP_K, -1);
		int minSupport = this.conf.getInt(KEY_MINSUP, -1);
		
		StringBuilder builder = new StringBuilder();
		
		builder.append("Here is LCM-over-Hadoop driver, finding ");
		
		if (k > 0) {
			builder.append("top-");
			builder.append(k);
			builder.append("-per-item ");
		}
		
		builder.append("itemsets (supported by at least ");
		builder.append(minSupport);
		builder.append(" transactions) from ");
		builder.append(this.input);
		builder.append(" (splitted in ");
		builder.append(g);
		builder.append(" groups), outputting them to ");
		builder.append(this.output);
		
		return builder.toString();
	}
	
	public int run(String[] args) throws Exception {
		System.out.println(toString());
		
		return 0;
	}
	
}
