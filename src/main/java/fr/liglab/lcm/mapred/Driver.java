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
 * Driver program for map-reduce'd mining 
 * 
 * Each job is a static method.
 */
public final class Driver {
	//////////////////// MANDATORY CONFIGURATION PROPERTIES ////////////////////
	
	public static final String KEY_INPUT    = "lcm.path.input";
	public static final String KEY_OUTPUT   = "lcm.path.output";
	public static final String KEY_MINSUP   = "lcm.minsup";
	public static final String KEY_NBGROUPS = "lcm.nbGroups";
	public static final String KEY_DO_TOP_K = "lcm.topK";
	
	//////////////////// OPTIONAL CONFIGURATION PROPERTIES ////////////////////
	
	/**
	 * Must be an actual class name from fr.liglab.lcm.mapred.groupers
	 */
	public static final String KEY_GROUPER_CLASS = "lcm.grouper";
	
	/**
	 * For profiling and testing only : the given groups'ID will be the only generated and mined sub-DB
	 */
	public static final String KEY_SINGLE_GROUP_ID = "lcm.single-group";
	
	/**
	 * For debugging : when set to an existing folder on task tracker, heap dump will be printed on OutOfMemoryError
	 */
	public static final String KEY_DUMP_ON_HEAP_EXN = "lcm.dump-path";
	
	/**
	 * Mining reducers gone multi-threaded (defaults to 1)
	 */
	public static final String KEY_NB_THREADS = "lcm.threads";
	
	/**
	 * If set, 1st phase of mining will be interrupted once the given item ID is outputted 
	 */
	public static final String KEY_STOP_AT = "lcm.mining.stopAt";
	
	/**
	 * Testing only : it changes manually ConcatenatedDataset's long transaction mode threshold
	 */
	public static final String KEY_LONG_TRANSACTION_MODE_THRESHOLD = "lcm.pptest-first.threshold";
	
	//////////////////// INTERNAL CONFIGURATION PROPERTIES ////////////////////
	
	/**
	 * this property will be filled after item counting
	 */
	public static final String KEY_REBASING_MAX_ID = "lcm.items.maxId";
	
	/**
	 * this property will be set by miningPhase1() if top-K boundaries have 
	 * actually been written to disk (and distcache)
	 */
	public static final String KEY_BOUNDS_IN_DISTCACHE = "lcm.bounds.written";
	
	
	protected final Configuration originalConf;
	protected final String input;
	protected final String output;
	
	/**
	 * All public KEY_* are expected in the provided configuration
	 * (except KEY_DO_TOP_K : if it's not set, all patterns will be mined)
	 */
	public Driver(Configuration configuration) {
		this.originalConf = configuration;
		
		this.input = this.originalConf.get(KEY_INPUT);
		this.output = this.originalConf.get(KEY_OUTPUT);
	}
	
	@Override
	public String toString() {
		int g = this.originalConf.getInt(KEY_NBGROUPS, -1);
		int k = this.originalConf.getInt(KEY_DO_TOP_K, -1);
		int minSupport = this.originalConf.getInt(KEY_MINSUP, -1);
		
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
		System.out.println(this.toString());
		
		String rebasingMapPath = this.output + "/" + DistCache.REBASINGMAP_DIRNAME;
		String rawPatternsPath = this.output + "/" + "rawMinedPatterns";
		String patternsPath = output + "/" + "topPatterns";
		
		if (genItemMap(this.originalConf, this.input, rebasingMapPath)) {
			
			Configuration confWithRebasing = new Configuration(this.originalConf);
			DistCache.copyToCache(confWithRebasing, rebasingMapPath);
			

			String patterns1 = rawPatternsPath + "/1";
			String patterns2 = rawPatternsPath + "/2";
			String subDBsPath = this.output + "/" + "subDBs";
			String boundsPath = this.output + "/" + DistCache.BOUNDS_DIRNAME;
			
			Configuration miningConf = new Configuration(this.originalConf);
			
			if (buildSubDBs(confWithRebasing, input, subDBsPath) &&
					miningPhase1(miningConf, subDBsPath, patterns1, boundsPath)) {
				
				if (this.originalConf.getInt(Driver.KEY_STOP_AT, -1) > 0) {
					return 0;
				}
				
				if (miningConf.getLong(KEY_BOUNDS_IN_DISTCACHE, -1) > 0) {
					DistCache.copyToCache(miningConf, boundsPath);
				}
				
				if (miningPhase2(miningConf, subDBsPath, patterns2) &&
						aggregateTopK(confWithRebasing, patternsPath, patterns1, patterns2)) {
					
					return 0;
				}
			}
		}
		
		return 1;
	}
	
	/**
	 * Item counting and rebasing job
	 * KEY_REBASING_MAX_ID will be added to "conf"
	 * @return true on success
	 */
	private static boolean genItemMap(Configuration conf, 
			final String input, final String output) 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		Job job = new Job(conf, "Computing frequent items mapping to groups, from "+input);
		job.setJarByClass(Driver.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input) );
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setMapperClass(ItemCountingMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(ItemAndSupportWritable.class);
		
		job.setReducerClass(ItemCountingReducer.class);
		job.setNumReduceTasks(1);
		
		boolean success = job.waitForCompletion(true);
		
		if (success) {
			CounterGroup counters = job.getCounters().getGroup(ItemCountingReducer.COUNTERS_GROUP);
			Counter rebasingMaxID = counters.findCounter(ItemCountingReducer.COUNTER_REBASING_MAX_ID);
			
			conf.setInt(KEY_REBASING_MAX_ID, (int) rebasingMaxID.getValue());
		}
		
		return success;
	}
	
	/**
	 * Aggregate patterns in order to ensure the K limit
	 * @param conf with rebasing map in distcache
	 * @param output
	 * @param inputs you may provide many !
	 * @return true on success
	 */
	private static boolean aggregateTopK(final Configuration conf, final String output, final String... inputs) 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		Job job = new Job(conf, "Aggregating top-K frequent itemsets to "+output);
		job.setJarByClass(Driver.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(ItemAndSupportWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(SupportAndTransactionWritable.class);
		
		for (String input : inputs) {
			FileInputFormat.addInputPath(job, new Path(input) );
		}
		
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setSortComparatorClass(ItemAndSupportWritable.SortComparator.class);
		job.setGroupingComparatorClass(ItemAndSupportWritable.ItemOnlyComparator.class);
		job.setPartitionerClass(AggregationPartitioner.class);
		job.setReducerClass(AggregationReducer.class);
		
		return job.waitForCompletion(true);
	}
	
	/**
	 * Two-phases mining : step 1/2
	 * This one has two outputs : mined patterns and discovered top-K support bounds, per item
	 * @param conf - if bounds have been written (in rare cases in may not happen), KEY_BOUNDS_IN_DISTCACHE will be set
	 * @param input path to sub-DBs file
	 * @param patternsPath
	 * @param boundsPath
	 * @return true on success
	 */
	private static boolean miningPhase1(Configuration conf,
			final String input, final String patternsPath, final String boundsPath) 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		conf.set(MiningTwoPhasesReducer.KEY_BOUNDS_PATH, "tmp/bounds");
		conf.setInt(MiningTwoPhasesReducer.KEY_PHASE_ID, 1);
		
		Job job = new Job(conf, "Two-phases mining : phase 1 over "+input);
		job.setJarByClass(Driver.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TransactionWritable.class);
		job.setOutputKeyClass(ItemAndSupportWritable.class);
		job.setOutputValueClass(SupportAndTransactionWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(patternsPath));
		
		MultipleOutputs.addNamedOutput(job, MiningTwoPhasesReducer.BOUNDS_OUTPUT_NAME, 
				SequenceFileOutputFormat.class, IntWritable.class, IntWritable.class);
		
		if (conf.getInt(Driver.KEY_STOP_AT, -1) > 0) {
			MultipleOutputs.addNamedOutput(job, MiningTwoPhasesReducer.TOK_SUPPORTS_DUMP_OUTPUT_NAME, 
					SequenceFileOutputFormat.class, IntWritable.class, TransactionWritable.class);
		}
		
		job.setReducerClass(MiningTwoPhasesReducer.class);
		
		if (job.waitForCompletion(true)) {
			
			Counter boundsCounter = job.getCounters().findCounter(MiningTwoPhasesReducer.COUNTER_GROUP, 
					MiningTwoPhasesReducer.COUNTER_BOUNDS_COUNT);
			
			if (boundsCounter.getValue() > 0) {
				FileSystem fs = FileSystem.get(conf);
				fs.rename(new Path(patternsPath+"/tmp"), new Path(boundsPath));
				
				conf.setLong(KEY_BOUNDS_IN_DISTCACHE, boundsCounter.getValue());
			}
			
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * Two-phases mining : step 2/2
	 * @param conf - expecting known support bounds in dist cache
	 * @param input path to sub-DBs file
	 * @param output
	 * @return true on success
	 */
	protected static boolean miningPhase2(Configuration conf, 
			final String input, final String output) 
			throws IOException, InterruptedException, ClassNotFoundException {

		conf.setInt(MiningTwoPhasesReducer.KEY_PHASE_ID, 2);
		Job job = new Job(conf, "Two-phases mining : phase 2 over "+input);
		job.setJarByClass(Driver.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TransactionWritable.class);
		job.setOutputKeyClass(ItemAndSupportWritable.class);
		job.setOutputValueClass(SupportAndTransactionWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setReducerClass(MiningTwoPhasesReducer.class);
		
		return job.waitForCompletion(true);
	}
	
	/**
	 * Create the sub-DBs file, sorted by groupID
	 * @param conf - expecting rebasing map in dist cache
	 * @return true on success
	 */
	protected static boolean buildSubDBs(Configuration conf, 
			final String input, final String output) 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		Job job = new Job(conf, "Two-phases mining : building sub-DBs from "+input);
		job.setJarByClass(Driver.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(TransactionWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input) );
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setMapperClass(MiningMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TransactionWritable.class);
		
		// then job defaults to shuffle'n'sorting to IdentityReducer, which is exactly what we want
		
		return job.waitForCompletion(true);
	}
}
