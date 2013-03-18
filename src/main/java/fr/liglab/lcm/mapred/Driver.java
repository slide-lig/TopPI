package fr.liglab.lcm.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import fr.liglab.lcm.mapred.writables.GIDandRebaseWritable;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;

/**
 * Driver methods for our 3 map-reduce jobs
 */
public class Driver {
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
		this.conf.setStrings(KEY_AGGREGATED_PATTERNS, output + "/" + "topPatterns");
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
	
	public int run() throws Exception {
		System.out.println(toString());
		
		if (genItemMapToCache() && miningJob()) {
			int k = this.conf.getInt(KEY_DO_TOP_K, -1);
			
			if (k > 0) {
				if (aggregateTopK()) {
					return 0;
				} else {
					return 1;
				}
			} else {
				return 0;
			}
		}
		
		return 1;
	}
	
	protected boolean genItemMapToCache() 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		String output = this.conf.getStrings(KEY_GROUPS_MAP)[0];
		
		Job job = new Job(conf, 
				"Computing frequent items mapping to groups, from "+this.input);
		
		job.setJarByClass(this.getClass());
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(GIDandRebaseWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(this.input) );
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setMapperClass(ItemCountingMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(ItemAndSupportWritable.class);
		
		job.setReducerClass(ItemGroupingReducer.class);
		job.setNumReduceTasks(1);
		
		boolean success = job.waitForCompletion(true);
		
		if (success) {
			DistCache.copyToCache(this.conf, output);
		}
		
		return success;
	}
	
	protected boolean miningJob() 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		String output = this.conf.getStrings(KEY_RAW_PATTERNS)[0];
		
		Job job = new Job(conf, "Mining frequent itemsets from "+this.input);
		
		job.setJarByClass(this.getClass());
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(ItemAndSupportWritable.class);
		job.setOutputValueClass(TransactionWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(this.input) );
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setMapperClass(MiningMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TransactionWritable.class);
		
		job.setReducerClass(MiningReducer.class);
		
		return job.waitForCompletion(true);
	}
	
	protected boolean aggregateTopK() 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		String input  = this.conf.getStrings(KEY_RAW_PATTERNS)[0];
		String output = this.conf.getStrings(KEY_AGGREGATED_PATTERNS)[0];
		
		Job job = new Job(conf, "Aggregating top-K frequent itemsets from "+this.input);
		
		job.setJarByClass(this.getClass());
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(ItemAndSupportWritable.class);
		job.setOutputValueClass(TransactionWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input) );
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setSortComparatorClass(ItemAndSupportWritable.SortComparator.class);
		job.setGroupingComparatorClass(ItemAndSupportWritable.ItemOnlyComparator.class);
		job.setPartitionerClass(AggregationPartitioner.class);
		job.setReducerClass(AggregationReducer.class);
		
		return job.waitForCompletion(true);
	}
}
