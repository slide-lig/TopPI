package fr.liglab.mining.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import fr.liglab.mining.TopLCM;
import fr.liglab.mining.TopLCM.TopLCMCounters;
import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.internals.FrequentsIterator;
import fr.liglab.mining.internals.FrequentsIteratorRenamer;
import fr.liglab.mining.internals.Selector;
import fr.liglab.mining.io.PatternSortCollector;
import fr.liglab.mining.io.PatternsCollector;
import fr.liglab.mining.mapred.writables.ConcatenatedTransactionsWritable;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;

public class MiningReducer extends
		Reducer<IntWritable, ConcatenatedTransactionsWritable, IntWritable, SupportAndTransactionWritable> {
	
	static final String BOUNDS_OUTPUT_NAME = "bounds";
	static final String KEY_BOUNDS_PATH = "lcm.bound.path";
	static final String KEY_COLLECT_NON_GROUP = "lcm.collect.outgroup";
	
	private int[] reverseRebasing = null;
	private MultipleOutputs<IntWritable, SupportAndTransactionWritable> sideOutputs = null;
	
	private enum MyCounter {MINING_TIME};
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		if (this.reverseRebasing != null) {
			return;
		}
		
		Configuration conf = context.getConfiguration();
		this.reverseRebasing = DistCache.readReverseRebasing(conf);
		
		if (conf.getInt(TopLCMoverHadoop.KEY_METHOD, 0) == 2) {
			if (conf.get(KEY_BOUNDS_PATH) != null) {
				this.sideOutputs = new MultipleOutputs<IntWritable, SupportAndTransactionWritable>(context);
			}
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
				, maxId, this.reverseRebasing, conf.getInt(TopLCMoverHadoop.KEY_METHOD, 0) == 0); // otherwise we give starters: don't compress renaming
		
		context.progress();
		
		if (conf.get(TopLCMoverHadoop.KEY_SUB_DB_ONLY, "").length() > 0) {
			return;
		}
		
		Grouper grouper = new Grouper(nbGroups, maxId);
		FrequentsIterator collected;
		
		if (conf.getInt(TopLCMoverHadoop.KEY_METHOD, 0) == 1) {
			// collect all
			collected = initState.counters.getExtensionsIterator();
			collected = new FrequentsIteratorRenamer(collected, initState.counters.getReverseRenaming());
		} else if (conf.getBoolean(KEY_COLLECT_NON_GROUP, false)) {
			collected = grouper.getNonGroupItems(gid);
			collected = new FrequentsIteratorRenamer(collected, this.reverseRebasing);
		} else {
			// collect group
			collected = grouper.getGroupItems(gid);
			collected = new FrequentsIteratorRenamer(collected, this.reverseRebasing);
		}
		
		PerItemTopKHadoopCollector topKcoll = new PerItemTopKHadoopCollector(context, k, maxId, collected);
		
		topKcoll.preloadBounds(DistCache.readPerItemBounds(conf));
		
		Selector chain = topKcoll.asSelector();
		
		if (conf.getInt(TopLCMoverHadoop.KEY_METHOD, 0) > 0) { // start group
			// startersSelector doesn't copy itself, so this only works if we call appendSelector only once
			chain = grouper.getStartersSelector(chain, gid);
		}
		
		initState.appendSelector(chain);
		
		PatternsCollector collector = topKcoll;

		if (conf.getBoolean(TopLCMoverHadoop.KEY_SORT_PATTERNS, false)) {
			collector = new PatternSortCollector(collector);
		}
		
		TopLCM miner = new TopLCM(collector, 1);
		miner.setHadoopContext(context);

		long chrono = System.currentTimeMillis();
		miner.lcm(initState);
		chrono = (System.currentTimeMillis() - chrono) / 1000;
		
		context.progress();
		long nbPatterns = collector.close();
		
		if (conf.getInt(TopLCMoverHadoop.KEY_METHOD, 0) == 2 && conf.get(KEY_BOUNDS_PATH) != null) {
			context.progress();
			topKcoll.writeTopKBounds(this.sideOutputs, BOUNDS_OUTPUT_NAME, conf.get(KEY_BOUNDS_PATH), minsup);
		}
		
		if (ExplorationStep.verbose) {
			HashMap<String, Long> logged = new HashMap<String,Long>();
			logged.put("gid", new Long(gid));
			logged.put("nbPatterns", nbPatterns);
			logged.put("miningTime", chrono);
			System.err.println(miner.toString(logged));
		}
		
		for (Entry<TopLCMCounters, Long> entry : miner.getCounters().entrySet()) {
			Counter counter = context.getCounter(entry.getKey());
			counter.increment(entry.getValue());
		}
		context.getCounter(MyCounter.MINING_TIME).increment(chrono);
		
		context.progress();
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		if (this.sideOutputs != null) {
			this.sideOutputs.close();
		}
	}
}





