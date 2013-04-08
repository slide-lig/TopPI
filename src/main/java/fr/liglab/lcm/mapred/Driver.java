package fr.liglab.lcm.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.SupportAndTransactionWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;

/**
 * Driver methods for our 3 map-reduce jobs
 */
public class Driver {
	//////////////////// MANDATORY CONFIGURATION PROPERTIES ////////////////////
	
	public static final String KEY_INPUT    = "lcm.path.input";
	public static final String KEY_OUTPUT   = "lcm.path.output";
	public static final String KEY_MINSUP   = "lcm.minsup";
	public static final String KEY_NBGROUPS = "lcm.nbGroups";
	public static final String KEY_DO_TOP_K = "lcm.topK";
	
	//////////////////// OPTIONAL CONFIGURATION PROPERTIES ////////////////////
	
	public static final String KEY_GROUPER_CLASS = "lcm.grouper";
	public static final String KEY_MINING_ALGO = "lcm.mapred.algo";
	public static final String KEY_SINGLE_GROUP_ID = "lcm.single-group";
	public static final String KEY_DUMP_ON_HEAP_EXN = "lcm.dump-path";
	
	
	//////////////////// INTERNAL CONFIGURATION PROPERTIES ////////////////////
	
	/**
	 * property key for item-counter-n-grouper output path
	 */
	static final String KEY_GROUPS_MAP = "lcm.path.groupsMap";
	
	/**
	 * property key for mining output path
	 */
	static final String KEY_RAW_PATTERNS = "lcm.path.rawpatterns";

	/**
	 * property key for aggregated patterns' output path
	 */
	static final String KEY_AGGREGATED_PATTERNS = "lcm.path.aggregated";

	/**
	 * property key for path of stored sub-DBs (for two-phases mining)
	 */
	static final String KEY_SUB_DBS = "lcm.path.subDBs";
	
	/**
	 * this property will be filled after item counting
	 */
	public static final String KEY_REBASING_MAX_ID = "lcm.items.maxId";
	
	
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
		
		this.conf.setStrings(KEY_GROUPS_MAP, this.output + "/" + DistCache.REBASINGMAP_DIRNAME);
		this.conf.setStrings(KEY_RAW_PATTERNS, this.output + "/" + "rawMinedPatterns");
		this.conf.setStrings(KEY_AGGREGATED_PATTERNS, output + "/" + "topPatterns");
		this.conf.setStrings(KEY_SUB_DBS, output + "/" + "subDBs");
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
		
		if (genItemMapToCache()) {
			
			String miningAlgo = this.conf.get(KEY_MINING_ALGO, "");
			
			if ("1".equals(miningAlgo)) {
				if (miningJob(false) && aggregateTopK()) {
					return 0;
				}
			} else if ("2".equals(miningAlgo)) {
				if (miningJob(true)) {
					return 0;
				}
			} else { // algo "3"
				if (twoPhasesMining() && aggregateTopK()) {
					return 0;
				}
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
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(this.input) );
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setMapperClass(ItemCountingMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(ItemAndSupportWritable.class);
		
		job.setReducerClass(ItemCountingReducer.class);
		job.setNumReduceTasks(1);
		
		boolean success = job.waitForCompletion(true);
		
		if (success) {
			DistCache.copyToCache(this.conf, output);
			CounterGroup counters = job.getCounters().getGroup(ItemCountingReducer.COUNTERS_GROUP);
			Counter rebasingMaxID = counters.findCounter(ItemCountingReducer.COUNTER_REBASING_MAX_ID);
			
			this.conf.setInt(KEY_REBASING_MAX_ID, (int) rebasingMaxID.getValue());
		}
		
		return success;
	}
	
	protected boolean miningJob(boolean useGroupOnly) 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		Job job = new Job(conf, "Mining frequent itemsets from "+this.input);
		
		job.setJarByClass(this.getClass());
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(ItemAndSupportWritable.class);
		job.setOutputValueClass(SupportAndTransactionWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(this.input) );
		
		job.setMapperClass(MiningMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TransactionWritable.class);
		
		String outputPath;
		if (useGroupOnly) {
			outputPath = this.conf.getStrings(KEY_AGGREGATED_PATTERNS)[0];
			job.setReducerClass(MiningGroupOnlyReducer.class);
		} else {
			outputPath = this.conf.getStrings(KEY_RAW_PATTERNS)[0];
			job.setReducerClass(MiningReducer.class);
		}
		
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
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
		job.setMapOutputKeyClass(ItemAndSupportWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(SupportAndTransactionWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input) );
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setSortComparatorClass(ItemAndSupportWritable.SortComparator.class);
		job.setGroupingComparatorClass(ItemAndSupportWritable.ItemOnlyComparator.class);
		job.setPartitionerClass(AggregationPartitioner.class);
		job.setReducerClass(AggregationReducer.class);
		
		return job.waitForCompletion(true);
	}
	
	/**
	 * It's a 3-jobs mining, actually
	 */
	protected boolean twoPhasesMining() 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		String outputFolder = this.conf.get(KEY_RAW_PATTERNS);
		String patternsPhase1Folder = outputFolder + "/patterns-phase1";
		String patternsPhase2Folder = outputFolder + "/patterns-phase2";
		String boundsPath = this.output + DistCache.BOUNDS_DIRNAME;
		
		if (buildSubDBs() && miningPhase1(patternsPhase1Folder, boundsPath)) {
			Configuration phase2conf = new Configuration(this.conf);
			
			DistCache.copyToCache(phase2conf, boundsPath);
			return miningPhase2(phase2conf, patternsPhase2Folder);
		}
		
		return false;
	}
	
	private boolean miningPhase2(Configuration myConf, String outputFolder) 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		Job job = new Job(myConf, "Two-phases mining : phase 2 over "+this.input);
		job.setJarByClass(this.getClass());
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(ItemAndSupportWritable.class);
		job.setOutputValueClass(SupportAndTransactionWritable.class);
		
		String input = myConf.get(KEY_SUB_DBS);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(outputFolder));
		
		//job.setReducerClass(TwoPhasesMiningReducer.class);
		
		return job.waitForCompletion(true);
	}

	protected boolean miningPhase1(String outputFolder, String boundsFolder) 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		this.conf.set(MiningReducerPhase1.KEY_BOUNDS_PATH, "tmp/");
		
		Job job = new Job(this.conf, "Two-phases mining : phase 1 over "+this.input);
		job.setJarByClass(this.getClass());
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(ItemAndSupportWritable.class);
		job.setOutputValueClass(SupportAndTransactionWritable.class);
		
		String input = this.conf.get(KEY_SUB_DBS);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(outputFolder));
		
		MultipleOutputs.addNamedOutput(job, MiningReducerPhase1.BOUNDS_OUTPUT_NAME, 
				SequenceFileOutputFormat.class, IntWritable.class, IntWritable.class);
		
		job.setReducerClass(MiningReducerPhase1.class);
		
		if (job.waitForCompletion(true)) {
			FileSystem fs = FileSystem.get(conf);
			fs.rename(new Path(outputFolder+"/tmp"), new Path(boundsFolder));
			
			return true;
		} else {
			return false;
		}
	}

	protected boolean buildSubDBs() 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		Job job = new Job(this.conf, "Two-phases mining : building sub-DBs from "+this.input);
		
		job.setJarByClass(this.getClass());
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(TransactionWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(this.input) );
		
		String outputFolder = this.conf.get(KEY_SUB_DBS);
		FileOutputFormat.setOutputPath(job, new Path(outputFolder));
		
		job.setMapperClass(MiningMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TransactionWritable.class);
		
		// then job defaults to shuffle'n'sorting to IdentityReducer, which is exactly what we want
		
		return job.waitForCompletion(true);
	}
}
