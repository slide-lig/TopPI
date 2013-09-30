package fr.liglab.mining.mapred;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.mining.TopLCM;
import fr.liglab.mining.TopLCM.TopLCMCounters;
import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.internals.FrequentsIterator;
import fr.liglab.mining.internals.FrequentsIteratorRenamer;
import fr.liglab.mining.io.PatternSortCollector;
import fr.liglab.mining.io.PatternsCollector;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.mapred.writables.ConcatenatedTransactionsWritable;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;

public class MiningSinglePassReducer extends
		Reducer<IntWritable, ConcatenatedTransactionsWritable, NullWritable, SupportAndTransactionWritable> {
	
	private int[] reverseRebasing = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		if (this.reverseRebasing == null) {
			Configuration conf = context.getConfiguration();
			this.reverseRebasing = DistCache.readReverseRebasing(conf);
		}
	}
	
	@Override
	protected void reduce(IntWritable gidW, 
			Iterable<ConcatenatedTransactionsWritable> transactions, Context context)
			throws IOException, InterruptedException {
		
		final int gid = gidW.get();
		final Configuration conf = context.getConfiguration();

		ExplorationStep.verbose = conf.getBoolean(TopLCMoverHadoop.KEY_VERBOSE, false);
		ExplorationStep.ultraVerbose = conf.getBoolean(TopLCMoverHadoop.KEY_ULTRA_VERBOSE, false);
		
		final int k        = conf.getInt(TopLCMoverHadoop.KEY_K, 1);
		final int minsup   = conf.getInt(TopLCMoverHadoop.KEY_MINSUP, 1000);
		final int maxId    = conf.getInt(TopLCMoverHadoop.KEY_REBASING_MAX_ID, 1);
		final int nbGroups = conf.getInt(TopLCMoverHadoop.KEY_NBGROUPS, 1);

		// yeah it's ugly, but SubDatasetConverter holds a copy of the dataset so we'd 
		// rather suggest to the VM this is a short-lasting object
		ExplorationStep initState = new ExplorationStep(minsup, 
				new SubDatasetConverter(transactions.iterator())
				, maxId, this.reverseRebasing);
		
		context.progress();
		
		Grouper grouper = new Grouper(nbGroups, maxId);
		
		PatternsCollector collector = new HadoopPatternsCollector(context);

		if (conf.getBoolean(TopLCMoverHadoop.KEY_SORT_PATTERNS, false)) {
			collector = new PatternSortCollector(collector);
		}
		
		FrequentsIterator groupItems = grouper.getGroupItems(gid);
		groupItems = new FrequentsIteratorRenamer(groupItems, this.reverseRebasing);
		PerItemTopKCollector topKcoll = new PerItemTopKCollector(collector, k, maxId, groupItems);
		
		topKcoll.setInfoMode(conf.getBoolean(TopLCMoverHadoop.KEY_PATTERNS_INFO, false));
		
		TopLCM miner = new TopLCM(topKcoll, 1);
		miner.setHadoopContext(context);
		miner.lcm(initState);
		
		context.progress();
		topKcoll.close();
		
		for (Entry<TopLCMCounters, Long> entry : miner.getCounters().entrySet()) {
			Counter counter = context.getCounter(entry.getKey());
			counter.increment(entry.getValue());
		}
		
		context.progress();
	}
}





