/*
	This file is part of TopPI - see https://github.com/slide-lig/TopPI/
	
	Copyright 2016 Martin Kirchgessner, Vincent Leroy, Alexandre Termier, Sihem Amer-Yahia, Marie-Christine Rousset, Université Grenoble Alpes, LIG, CNRS
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	 http://www.apache.org/licenses/LICENSE-2.0
	 
	or see the LICENSE.txt file joined with this program.
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package fr.liglab.mining.mapred;

import java.io.IOException;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import fr.liglab.mining.TopPIcli;
import fr.liglab.mining.mapred.writables.ConcatenatedTransactionsWritable;
import fr.liglab.mining.mapred.writables.ItemAndSupportWritable;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;

/**
 * The Hadoop driver.
 */
public class TopPIoverHadoop extends Configured implements Tool {

	public static final String FILTERED_DIRNAME = "topPI_internal_FilteredInput";

	// //////////////////MANDATORY CONFIGURATION PROPERTIES ////////////////////
	public static final String KEY_INPUT = "toppi.path.input";
	public static final String KEY_MINSUP = "toppi.minsup";
	public static final String KEY_NBGROUPS = "toppi.nbGroups";
	public static final String KEY_K = "toppi.topK";

	// ////////////////// OPTIONS ////////////////////
	public static final String KEY_VERBOSE = "toppi.verbose";
	public static final String KEY_ULTRA_VERBOSE = "toppi.verbose.ultra";
	public static final String KEY_COMBINED_TRANS_SIZE = "toppi.subdbs.combined-size";
	// only works fine with method 0 !
	public static final String KEY_SUB_DB_ONLY = "toppi.only.subdbs";
	public static final String KEY_SINGLE_GROUP = "toppi.only.group";
	// threads per reducer task - defaults to 1
	public static final String KEY_NB_THREADS = "toppi.reducer.threads";
	// enables the 3-passes preliminary jobs - set to true if you have more than
	// 2 million items
	public static final String KEY_MANY_ITEMS_MODE = "toppi.items.many";
	
	// set it to k' < k if the final step should over-filter by correlation with the key item
	public static final String KEY_CORRELATION_RESULTS = "toppi.pval.k";

	// ////////////////// INTERNAL CONFIGURATION PROPERTIES ////////////////////

	/**
	 * this property will be filled after item counting
	 */
	public static final String KEY_REBASING_MAX_ID = "toppi.items.maxId";

	private String input;
	private String outputPrefix;
	
	@Override
	public int run(String[] rawArgs) throws Exception {

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(TopPIcli.getOptions(), rawArgs);
		String[] args = cmd.getArgs();
		
		if (args.length != 3) {
			throw new IllegalArgumentException("Output's prefix path must be provided when using Hadoop");
		}
		
		if (cmd.hasOption('s')) {
			System.err.println("Hadoop version does not support itemset sorting");
			System.exit(1);
		}
		
		this.input = args[0];
		this.outputPrefix = args[2];
		
		int k = Integer.parseInt(cmd.getOptionValue('k'));
		
		Configuration conf = this.getConf();
		conf.set(TopPIoverHadoop.KEY_INPUT, args[0]);
		conf.setInt(TopPIoverHadoop.KEY_MINSUP, Integer.parseInt(args[1]));
		conf.setInt(TopPIoverHadoop.KEY_K, k);
		conf.setInt(TopPIoverHadoop.KEY_NBGROUPS, Integer.parseInt(cmd.getOptionValue('g')));

		conf.setBoolean(TopPIoverHadoop.KEY_VERBOSE, cmd.hasOption('v'));
		conf.setBoolean(TopPIoverHadoop.KEY_ULTRA_VERBOSE, cmd.hasOption('V'));
		conf.setBoolean(TopPIoverHadoop.KEY_MANY_ITEMS_MODE, cmd.hasOption('B'));
		
		if (cmd.hasOption('c')) {
			int c = Integer.parseInt(cmd.getOptionValue('c'));
			if (c > k){
				throw new IllegalArgumentException("c must be smaller or equal to k");
			}
			conf.setInt(TopPIoverHadoop.KEY_CORRELATION_RESULTS, c);
		}
		
		String itemCountPath = this.outputPrefix + "/" + "itemCounts";
		String filteredInputPath = this.outputPrefix + "/" + FILTERED_DIRNAME;
		String rebasingMapPath = this.outputPrefix + "/" + "rebasing";
		String topKperItemPath = this.outputPrefix + "/" + "topPatterns";
		String rawPatternsPath = this.outputPrefix + "/" + "rawPatterns";
		String boundsPath = this.outputPrefix + "/" + "perItemBounds";
		if (conf.getBoolean(KEY_MANY_ITEMS_MODE, false)) {
			if (!bigItemCount(itemCountPath) || !genBigItemMap(itemCountPath, rebasingMapPath)) {
				return 1;
			}
			if (!filterInput(filteredInputPath, rebasingMapPath)) {
				return 1;
			}
			this.input = filteredInputPath;
		} else {
			if (!genItemMap(rebasingMapPath)) {
				return 1;
			}
		}

		rawPatternsPath = rawPatternsPath + "/";
		if (mineFirstPass(rawPatternsPath + "1", boundsPath, rebasingMapPath)
				&& mineSecondPass(rawPatternsPath + "2", boundsPath, rebasingMapPath)
				&& aggregate(topKperItemPath, rawPatternsPath + "1", rawPatternsPath + "2")) {

			return 0;
		}

		return 1;
	}

	/**
	 * Restricts a pattern set to the top-K-per-item
	 * 
	 * @param input
	 * @param output
	 * @return true on success
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private boolean aggregate(String output, String... inputs) throws IOException, ClassNotFoundException,
			InterruptedException {

		Job job = Job.getInstance(this.getConf(), "Per-item top-k aggregation of itemsets from " + input);
		job.setJarByClass(TopPIoverHadoop.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(SupportAndTransactionWritable.class);
		
		for (String input : inputs) {
			FileInputFormat.addInputPath(job, new Path(input));
		}
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(AggregationMapper.class);
		job.setMapOutputKeyClass(ItemAndSupportWritable.class);
		job.setMapOutputValueClass(SupportAndTransactionWritable.class);

		job.setSortComparatorClass(ItemAndSupportWritable.SortComparator.class);
		job.setGroupingComparatorClass(ItemAndSupportWritable.ItemOnlyComparator.class);
		job.setPartitionerClass(AggregationReducer.AggregationPartitioner.class);
		
		if (this.getConf().getInt(TopPIoverHadoop.KEY_CORRELATION_RESULTS, -1)>=0) {
			job.setReducerClass(AggregationByCorrelationReducer.class);
			DistCache.copyToCache(job, this.input);
		} else {
			job.setReducerClass(AggregationReducer.class);
		}
		
		job.setNumReduceTasks(this.getConf().getInt(KEY_NBGROUPS, 1));

		return job.waitForCompletion(true);
	}

	/**
	 * Item counting and rebasing job; its output is the rebasing map
	 * KEY_REBASING_MAX_ID will be set in current Configuration
	 * 
	 * @return true on success
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private boolean genItemMap(String output) throws IOException, ClassNotFoundException, InterruptedException {

		Job job = Job.getInstance(this.getConf(), "Computing frequent items mapping to groups, from " + this.input);
		job.setJarByClass(TopPIoverHadoop.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(this.input));
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

			this.getConf().setInt(KEY_REBASING_MAX_ID, (int) rebasingMaxID.getValue());
		}

		return success;
	}

	/**
	 * Mining, pass 1/2 (start group/collect group)
	 * 
	 * @param output
	 * @param bounds
	 *            path of the bounds side-file
	 * @return true on success
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private boolean mineFirstPass(String output, String bounds, String rebasingMapPath) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration config = this.getConf();
		
		Job job = Job.getInstance(config, "Mining (first pass) " + this.input);
		job.setJarByClass(TopPIoverHadoop.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(rebasingMapPath));

		job.setMapperClass(MiningMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(MiningReducer.class);
		job.setNumReduceTasks(config.getInt(KEY_NBGROUPS, 1));

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(SupportAndTransactionWritable.class);

		MultipleOutputs.addNamedOutput(job, MinerWrapper.BOUNDS_OUTPUT_NAME, SequenceFileOutputFormat.class,
				IntWritable.class, IntWritable.class);

		job.getConfiguration().set(MinerWrapper.KEY_BOUNDS_PATH, "tmp/bounds");

		DistCache.copyToCache(job, rebasingMapPath);
		DistCache.copyToCache(job, this.input);

		if (job.waitForCompletion(true)) {
			FileSystem fs = FileSystem.get(config);

			try {
				fs.rename(new Path(output + "/tmp"), new Path(bounds));
			} catch (IOException e) {
				Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).warning(
						"Can't rename bounds file, maybe none has been found." + e);
			}

			fs.close();

			return true;
		}

		return false;
	}

	/**
	 * Mining, pass 2/2 (start group/collect non-group)
	 * 
	 * @param output
	 * @param bounds
	 *            path of the bounds side-file
	 * @return true on success
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private boolean mineSecondPass(String output, String bounds, String rebasingMapPath) throws IOException,
			ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(this.getConf(), "Mining (second pass) " + this.input);
		job.setJarByClass(TopPIoverHadoop.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(rebasingMapPath));

		job.setMapperClass(MiningMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(MiningReducer.class);
		job.setNumReduceTasks(this.getConf().getInt(KEY_NBGROUPS, 1));

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(SupportAndTransactionWritable.class);

		DistCache.copyToCache(job, rebasingMapPath);
		DistCache.copyToCache(job, bounds);
		DistCache.copyToCache(job, this.input);

		job.getConfiguration().setBoolean(MinerWrapper.KEY_COLLECT_NON_GROUP, true);

		return job.waitForCompletion(true);
	}

	private boolean bigItemCount(String output) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(this.getConf(), "Counting items from " + this.input);
		job.setJarByClass(TopPIoverHadoop.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(this.input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(ItemBigCountingMapper.class);
		job.setReducerClass(ItemBigCountingReducer.class);

		boolean success = job.waitForCompletion(true);

		if (success) {
			Counter rebasingMaxID = job.getCounters().findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS);
			this.getConf().setInt(KEY_REBASING_MAX_ID, (int) rebasingMaxID.getValue());
		}

		return success;
	}

	private boolean genBigItemMap(String input, String output) throws IOException, ClassNotFoundException,
			InterruptedException {
		Job job = Job.getInstance(this.getConf(), "Computing items remapping for " + this.input);
		job.setJarByClass(TopPIoverHadoop.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(InverseMapper.class);
		job.setReducerClass(ItemBigRebasingReducer.class);
		job.setNumReduceTasks(1);

		return job.waitForCompletion(true);
	}

	private boolean filterInput(String output, String rebasingMapPath) throws IOException, ClassNotFoundException,
			InterruptedException {
		Job job = Job.getInstance(this.getConf(), "Computing items remapping for " + this.input);
		job.setJarByClass(TopPIoverHadoop.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(ConcatenatedTransactionsWritable.class);
		DistCache.copyToCache(job, rebasingMapPath);
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(FilteringMapper.class);
		job.setNumReduceTasks(0);

		return job.waitForCompletion(true);
	}
}
