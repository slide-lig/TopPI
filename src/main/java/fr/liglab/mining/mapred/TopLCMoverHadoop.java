package fr.liglab.mining.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import fr.liglab.mining.mapred.writables.ConcatenatedTransactionsWritable;
import fr.liglab.mining.mapred.writables.ItemAndSupportWritable;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;

/**
 * The Hadoop driver.
 */
public class TopLCMoverHadoop implements Tool {
	
	////////////////////MANDATORY CONFIGURATION PROPERTIES ////////////////////
	public static final String KEY_INPUT    = "toplcm.path.input";
	public static final String KEY_OUTPUT   = "toplcm.path.output";
	public static final String KEY_MINSUP   = "toplcm.minsup";
	public static final String KEY_NBGROUPS = "toplcm.nbGroups";
	public static final String KEY_K        = "toplcm.topK";
	
	
	//////////////////// OPTIONS ////////////////////
	public static final String KEY_VERBOSE         = "toplcm.verbose";
	public static final String KEY_ULTRA_VERBOSE   = "toplcm.verbose.ultra";
	public static final String KEY_SORT_PATTERNS   = "toplcm.patterns.sorted";
	public static final String KEY_UNIQUE_PATTERNS = "toplcm.patterns.unique";
	public static final String KEY_PATTERNS_INFO   = "toplcm.patterns.info";
	
	//////////////////// INTERNAL CONFIGURATION PROPERTIES ////////////////////
	
	/**
	 * this property will be filled after item counting
	 */
	public static final String KEY_REBASING_MAX_ID = "toplcm.items.maxId";
	
	
	
	private Configuration conf;
	private String input;
	private String outputPrefix;
	
	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public void setConf(Configuration c) {
		this.conf = c;
		this.input = c.get(KEY_INPUT);
		this.outputPrefix = c.get(KEY_OUTPUT);
	}
	
	public TopLCMoverHadoop(Configuration c) {
		this.setConf(c);
	}
	
	/**
	 * @param args is ignored - use Configuration instead
	 */
	@Override
	public int run(String[] args) throws Exception {
		String rebasingMapPath = this.outputPrefix + "/" + DistCache.REBASINGMAP_DIRNAME;
		String topKperItemPath = this.outputPrefix + "/" + "topPatterns";
		
		if (genItemMap(rebasingMapPath)) {
			DistCache.copyToCache(this.conf, rebasingMapPath);
			
			if (mineSinglePass(topKperItemPath)) {
				return 0;
			}
		}
		
		return 1;
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

		Job job = new Job(conf, "Mining (single-pass) "+this.input);
		job.setJarByClass(TopLCMoverHadoop.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
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

}