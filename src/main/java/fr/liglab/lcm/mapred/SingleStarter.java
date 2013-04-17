package fr.liglab.lcm.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fr.liglab.lcm.internals.ExtensionsIterator;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.SupportAndTransactionWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;

/**
 * should be used to resume a phase#1 mining ran with lcm.mining.stopAt
 */
public class SingleStarter extends Configured implements Tool{

	static final String KEY_PATH_TO_PATTERNS_SUPPORTS = "lcm.mapred.debug.topK-path";
	static final String KEY_STARTER = "lcm.mapred.debug.starter";
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new SingleStarter(), args);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 7 && args.length != 8) {
			System.err.println("USAGE: hadoop jar SingleStarter.jar -libjars trove4j-3.0.3.jar SUB_DBS_PATH MINSUP NBGROUPS K PATH_TO_KNOWN_BOUNDS MAX_ITEM STARTER [OUTPUT_PATH]");
			return 1;
		}
		
		String outputPath = null;
		
		if (args.length == 8) {
			outputPath = args[7];
		}
		
		return run(args[0], Integer.parseInt(args[1]), 
				Integer.parseInt(args[2]), Integer.parseInt(args[3]), 
				args[4], Integer.parseInt(args[5]), Integer.parseInt(args[6]), outputPath);
	}
	
	private int run(String input, int minsup, int nbgroups, int k, String boundsPath, int maxItemID, int starter, String output) 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		Configuration conf = getConf();
		
		conf.setInt(Driver.KEY_MINSUP, minsup);
		conf.setInt(Driver.KEY_DO_TOP_K, k);
		conf.setInt(Driver.KEY_NBGROUPS, nbgroups);
		conf.setInt(Driver.KEY_REBASING_MAX_ID, maxItemID);
		conf.setInt(MiningTwoPhasesReducer.KEY_PHASE_ID, 1);
		conf.set(KEY_PATH_TO_PATTERNS_SUPPORTS, boundsPath);
		conf.setInt(KEY_STARTER, starter);
		
		Job job = new Job(conf, "resuming mining");
		job.setJarByClass(this.getClass());
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		if (output == null) {
			job.setOutputFormatClass(NullOutputFormat.class);
		} else {
			job.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(job, new Path(output));
		}
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TransactionWritable.class);
		job.setOutputKeyClass(ItemAndSupportWritable.class);
		job.setOutputValueClass(SupportAndTransactionWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		
		job.setReducerClass(MiningTwoPhasesReducer.class);
		
		if (job.waitForCompletion(true)) {
			return 0;
		}
		
		return 1;
		
	}
	
	
	
	
	//////////////////////// specific tooling /////////////////////////////////

	@SuppressWarnings("deprecation")
	static void preFillCollector(Configuration conf, PerItemTopKHadoopCollector collector) throws IOException {
		IntWritable key = new IntWritable();
		TransactionWritable value = new TransactionWritable();
		
		Path inputPath = new Path(conf.get(KEY_PATH_TO_PATTERNS_SUPPORTS));
		
		FileSystem fs = FileSystem.get(conf);
		Path qualifiedPath = fs.makeQualified(inputPath);
		
		FileStatus[] statuses = fs.listStatus(qualifiedPath);
		
		for (Path file : FileUtil.stat2Paths(statuses)) {
			String stringified = file.toString();
			int lastSlash = stringified.lastIndexOf('/');
			
			if (stringified.charAt(lastSlash+1) != '_') {
				Reader reader = new Reader(fs, file, conf);
				
				while(reader.next(key, value)) {
					collector.init(key.get(), value.get());
				}
				
				reader.close();
			}
		}
	}
	
	static class SingleExtensionIterator implements ExtensionsIterator {
		
		private final int[] realcandidates;
		private int extension;
		
		public SingleExtensionIterator(int singleExtension, int[] sortedFrequents) {
			this.realcandidates = sortedFrequents;
			this.extension = singleExtension;
		}
		
		public int[] getSortedFrequents() {
			return this.realcandidates;
		}

		public int getExtension() {
			if (this.extension > 0) {
				int val = this.extension;
				this.extension = -1;
				return val;
			}
			
			return -1;
		}
	}
}
