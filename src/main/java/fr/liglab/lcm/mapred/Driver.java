package fr.liglab.lcm.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class Driver extends Configured implements Tool {
	
	protected final String input;
	protected final String output;
	protected final int minSupport;
	protected final int nbGroups;
	protected final Integer k;
	
	public Driver(String in, String out, int minSup, int groups, Integer k) {
		this.input = in;
		this.output = out;
		this.minSupport = minSup;
		this.nbGroups = groups;
		this.k = k;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		
		builder.append("Here is LCM-over-Hadoop driver, finding ");
		
		if (this.k != null) {
			builder.append("top-");
			builder.append(this.k);
			builder.append("-per-item ");
		}
		
		builder.append("itemsets (supported by at least ");
		builder.append(this.minSupport);
		builder.append(" transactions) from ");
		builder.append(this.input);
		builder.append(" (splitted in ");
		builder.append(this.nbGroups);
		builder.append(" groups), outputting them to ");
		builder.append(this.output);
		
		return builder.toString();
	}
	
	public int run(String[] args) throws Exception {
		System.out.println(toString());
		return 0;
	}
	
}
