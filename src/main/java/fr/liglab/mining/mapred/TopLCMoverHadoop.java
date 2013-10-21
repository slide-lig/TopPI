package fr.liglab.mining.mapred;

import java.io.IOException;
import java.util.logging.Logger;

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
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import fr.liglab.mining.mapred.writables.ConcatenatedTransactionsWritable;
import fr.liglab.mining.mapred.writables.ItemAndSupportWritable;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;

/**
 * The Hadoop driver.
 */
public class TopLCMoverHadoop {
	
	////////////////////MANDATORY CONFIGURATION PROPERTIES ////////////////////
	public static final String KEY_INPUT    = "toplcm.path.input";
	public static final String KEY_OUTPUT   = "toplcm.path.output";
	public static final String KEY_MINSUP   = "toplcm.minsup";
	public static final String KEY_NBGROUPS = "toplcm.nbGroups";
	public static final String KEY_K        = "toplcm.topK";
	
	
	//////////////////// OPTIONS ////////////////////
	public static final String KEY_VERBOSE       = "toplcm.verbose";
	public static final String KEY_ULTRA_VERBOSE = "toplcm.verbose.ultra";
	public static final String KEY_SORT_PATTERNS = "toplcm.patterns.sorted";
	public static final String KEY_SUB_DB_ONLY   = "toplcm.only.subdbs";
	public static final String KEY_COMBINED_TRANS_SIZE = "toplcm.subdbs.combined-size";
	public static final String KEY_METHOD = "toplcm.method";
	public static final String KEY_SINGLE_GROUP = "toplcm.only.group";
	
	//////////////////// INTERNAL CONFIGURATION PROPERTIES ////////////////////
	
	/**
	 * this property will be filled after item counting
	 */
	public static final String KEY_REBASING_MAX_ID = "toplcm.items.maxId";
	
	private Configuration conf;
	private String input;
	private String outputPrefix;
	
	public Configuration getConf() {
		return this.conf;
	}
	
	public void setConf(Configuration c) {
		this.conf = c;
		this.input = c.get(KEY_INPUT);
		this.outputPrefix = c.get(KEY_OUTPUT);
	}
	
	public TopLCMoverHadoop(Configuration c) {
		this.setConf(c);
	}
	
	public int run() throws Exception {
		String rebasingMapPath = this.outputPrefix + "/" + DistCache.REBASINGMAP_DIRNAME;
		String topKperItemPath = this.outputPrefix + "/" + "topPatterns";
		String rawPatternsPath = this.outputPrefix + "/" + "rawPatterns";
		String subDBsPath      = this.outputPrefix + "/" + "subDBs";
		String boundsPath      = this.outputPrefix + "/" + "bounds";
		
		if (genItemMap(rebasingMapPath)) {
			DistCache.copyToCache(this.conf, rebasingMapPath);
			
			if (this.conf.get(KEY_SUB_DB_ONLY, "").length() > 0) {
				if (justCreateSubDBs()) {
					return 0;
				}
			} else {
				switch (this.conf.getInt(KEY_METHOD, 0)) {
				case 1:
					// Algo 1: restrict starters to group's items, collect all item's top-Ks, aggregate
					if (mineSinglePass(rawPatternsPath) && aggregate(rawPatternsPath, topKperItemPath)) {
						return 0;
					}
					break;
				
				case 2:
					// Algo 2: 2 phases-mining
					rawPatternsPath = rawPatternsPath + "/";
					if (buildSubDBs(subDBsPath) && mineFirstPass(subDBsPath, rawPatternsPath + "1", boundsPath)) {
						// put bounds in DistCache
						// mining phase 2
						// aggregate
					}
					break;

				default: // Algo 0: use all available starters but restrict collector to group's items. 
					// doesn't need aggregation
					if (mineSinglePass(topKperItemPath)) {
						return 0;
					}
					break;
				}
			}
		}
		
		return 1;
	}
	
	/**
	 * Restricts a pattern set to the top-K-per-item
	 * @param input
	 * @param output
	 * @return true on success
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	private boolean aggregate(String input, String output) 
			throws IOException, ClassNotFoundException, InterruptedException {
		
		Job job = new Job(conf, "Per-item top-k aggregation of itemsets from "+input);
		job.setJarByClass(TopLCMoverHadoop.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(SupportAndTransactionWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input) );
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setMapperClass(AggregationMapper.class);
		job.setMapOutputKeyClass(ItemAndSupportWritable.class);
		job.setMapOutputValueClass(SupportAndTransactionWritable.class);
		
		job.setSortComparatorClass(ItemAndSupportWritable.SortComparator.class);
		job.setGroupingComparatorClass(ItemAndSupportWritable.ItemOnlyComparator.class);
		job.setPartitionerClass(AggregationReducer.AggregationPartitioner.class);
		job.setReducerClass(AggregationReducer.class);
		job.setNumReduceTasks(this.conf.getInt(KEY_NBGROUPS, 1));
		
		return job.waitForCompletion(true);
	}

	/**
	 * Item counting and rebasing job; its output is the rebasing map
	 * KEY_REBASING_MAX_ID will be set in current Configuration
	 * @return true on success
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	private boolean genItemMap(String output) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = new Job(conf, "Computing frequent items mapping to groups, from "+this.input);
		job.setJarByClass(TopLCMoverHadoop.class);
		
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
			CounterGroup counters = job.getCounters().getGroup(ItemCountingReducer.COUNTERS_GROUP);
			Counter rebasingMaxID = counters.findCounter(ItemCountingReducer.COUNTER_REBASING_MAX_ID);
			
			this.conf.setInt(KEY_REBASING_MAX_ID, (int) rebasingMaxID.getValue());
		}
		
		return success;
	}
	
	/**
	 * Mining in a single job : builds the sub-DBs, then each group will mine its items' top-K
	 * @param output 
	 * @return true on success
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	private boolean mineSinglePass(String output) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(this.conf, "Mining (single-pass) "+this.input);
		job.setJarByClass(TopLCMoverHadoop.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(SupportAndTransactionWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(this.input) );
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setMapperClass(MiningMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(ConcatenatedTransactionsWritable.class);
		
		job.setReducerClass(MiningSinglePassReducer.class);
		job.setNumReduceTasks(this.conf.getInt(KEY_NBGROUPS, 1));
		
		
		return job.waitForCompletion(true);
	}
	
	/**
	 * Mining, pass 1/2 (start group/collect group)
	 * @param in sub-DBs path
	 * @param output 
	 * @param bounds path of the bounds side-file
	 * @return true on success
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	private boolean mineFirstPass(String in, String output, String bounds) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(this.conf, "Mining (first pass) "+this.input);
		job.setJarByClass(TopLCMoverHadoop.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(SupportAndTransactionWritable.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(ConcatenatedTransactionsWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(in) );
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setReducerClass(MiningSinglePassReducer.class);
		job.setNumReduceTasks(this.conf.getInt(KEY_NBGROUPS, 1));
		
		MultipleOutputs.addNamedOutput(job, MiningSinglePassReducer.BOUNDS_OUTPUT_NAME, 
				SequenceFileOutputFormat.class, IntWritable.class, IntWritable.class);
		
		job.getConfiguration().set(MiningSinglePassReducer.KEY_BOUNDS_PATH, "tmp/bounds");
		
		if (job.waitForCompletion(true)) {
			FileSystem fs = FileSystem.get(conf);
			
			try {
				fs.rename(new Path(output+"/tmp"), new Path(bounds));
			} catch (IOException e) {
				Logger.getGlobal().warning("Can't rename bounds file, maybe none has been found.");
			}
			
			return true;
		}
		
		return false;
	}
	
	/**
	 * @param output 
	 * @return true on success
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	private boolean buildSubDBs(String output) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(this.conf, "Building sub-DBs over "+this.input);
		job.setJarByClass(TopLCMoverHadoop.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(ConcatenatedTransactionsWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(this.input) );
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setMapperClass(MiningMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(ConcatenatedTransactionsWritable.class);
		
		return job.waitForCompletion(true);
	}
	
	/**
	 * Test shuffle'n'sort
	 * @return true on success
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	private boolean justCreateSubDBs() throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job(conf, "Creating sub-DBs from "+this.input);
		job.setJarByClass(TopLCMoverHadoop.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(this.input) );
		
		job.setMapperClass(MiningMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(ConcatenatedTransactionsWritable.class);
		
		job.setReducerClass(FakeMiningReducer.class);
		job.setNumReduceTasks(1);
		
		return job.waitForCompletion(true);
	}
}
