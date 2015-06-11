package fr.liglab.mining.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.xml.ws.Holder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import fr.liglab.mining.CountersHandler.TopPICounters;
import fr.liglab.mining.TopPI;
import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.internals.FrequentsIterator;
import fr.liglab.mining.internals.FrequentsIteratorRenamer;
import fr.liglab.mining.internals.Selector;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;
import fr.liglab.mining.util.ProgressWatcherThread;

/**
 * This wrapper lets another class handle the initState instantiation
 * 
 * Be careful in here because 3 different bases co-exists :
 *  - the initial item indexing, used by the collector
 *  - the global, used by all maps in DistCache, where 0 is the most frequent item
 *  - a local one, used in initState, which is a compression of the global
 */
public final class MinerWrapper {

	static final String BOUNDS_OUTPUT_NAME = "bounds";
	static final String KEY_BOUNDS_PATH = "toppi.bound.path";
	static final String KEY_COLLECT_NON_GROUP = "toppi.collect.outgroup";
	
	private enum MyCounter {MINING_TIME};
	
	/**
	 * 
	 * @param gid
	 * @param context
	 * @param initState
	 * @param reverseRebasing
	 * @param sideOutputs can be null
	 * @param globalToInitial 
	 * @param renaming 
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	static void mining(int gid, ExplorationStep initState,
			org.apache.hadoop.mapreduce.Reducer<?,?,IntWritable, SupportAndTransactionWritable>.Context context,
			MultipleOutputs<IntWritable, SupportAndTransactionWritable> sideOutputs, int[] globalToInitial, Holder<int[]> renaming) throws IOException, InterruptedException {
		
		final Configuration conf = context.getConfiguration();

		ExplorationStep.verbose = conf.getBoolean(TopPIoverHadoop.KEY_VERBOSE, false);
		ExplorationStep.ultraVerbose = conf.getBoolean(TopPIoverHadoop.KEY_ULTRA_VERBOSE, false);
		
		final int k        = conf.getInt(TopPIoverHadoop.KEY_K, 1);
		final int minsup   = conf.getInt(TopPIoverHadoop.KEY_MINSUP, 1000);
		final int maxId    = conf.getInt(TopPIoverHadoop.KEY_REBASING_MAX_ID, 1);
		final int nbGroups = conf.getInt(TopPIoverHadoop.KEY_NBGROUPS, 1);
		
		ProgressWatcherThread coucou = new ProgressWatcherThread();
		coucou.setHadoopContext(context);
		coucou.start();
		
		if (conf.get(TopPIoverHadoop.KEY_SUB_DB_ONLY, "").length() > 0) {
			return;
		}
		
		Grouper grouper = new Grouper(nbGroups, maxId);
		FrequentsIterator collected;

		ExplorationStep.INSERT_UNCLOSED_UP_TO_ITEM = -1;
		ExplorationStep.INSERT_UNCLOSED_FOR_FUTURE_EXTENSIONS = false;
		
		if (conf.getBoolean(KEY_COLLECT_NON_GROUP, false)) {
			collected = grouper.getNonGroupItems(gid);
			collected = new FrequentsIteratorRenamer(collected, globalToInitial);
			ExplorationStep.EARLYCOLLECTION = false;
		} else {
			// collect group
			collected = grouper.getGroupItems(gid);
			collected = new FrequentsIteratorRenamer(collected, globalToInitial);
			ExplorationStep.EARLYCOLLECTION = true;
		}
		
		PerItemTopKHadoopCollector topKcoll = new PerItemTopKHadoopCollector(context, k, maxId, collected);
		
		topKcoll.preloadBounds(DistCache.readPerItemBounds(conf));
		
		Selector chain = topKcoll.asSelector();
		
		// startersSelector doesn't copy itself, so this only works if we call appendSelector only once
		chain = grouper.getStartersSelector(chain, gid, buildRenamingToGlobal(initState, renaming));
		
		initState.appendSelector(chain);
		
		coucou.setStartersIterator(initState.candidates);
		TopPIinHadoop miner = new TopPIinHadoop(topKcoll, conf.getInt(TopPIoverHadoop.KEY_NB_THREADS, 1));
		miner.setHadoopContext(context);

		long chrono = System.currentTimeMillis();
		miner.startMining(initState);
		chrono = (System.currentTimeMillis() - chrono) / 1000;
		
		context.progress();
		long nbPatterns = topKcoll.close();
		
		if (sideOutputs != null) {
			context.progress();
			topKcoll.writeTopKBounds(sideOutputs, BOUNDS_OUTPUT_NAME, conf.get(KEY_BOUNDS_PATH), minsup);
		}
		
		if (ExplorationStep.verbose) {
			HashMap<String, Long> logged = new HashMap<String,Long>();
			logged.put("gid", new Long(gid));
			logged.put("nbPatterns", nbPatterns);
			logged.put("miningTime", chrono);
			System.err.println(miner.toString(logged));
		}
		
		for (Entry<TopPICounters, Long> entry : miner.getCounters().entrySet()) {
			Counter counter = context.getCounter(entry.getKey());
			counter.increment(entry.getValue());
		}
		context.getCounter(MyCounter.MINING_TIME).increment(chrono);
		
		coucou.interrupt();
		coucou = null;
		context.progress();
		
	}

	private static int[] buildRenamingToGlobal(ExplorationStep initState, Holder<int[]> renaming) {
		int[] toCurrent = renaming.value;
		int[] toGlobal = new int[initState.counters.getMaxFrequent()+1];
		
		for (int i = 0; i < toCurrent.length; i++) {
			final int rebased = toCurrent[i];
			if (rebased >= 0) {
				toGlobal[rebased] = i;
			}
		}
		
		return toGlobal;
	}
	
	private static final class TopPIinHadoop extends TopPI {

		public TopPIinHadoop(PerItemTopKCollector patternsCollector, int nbThreads) {
			super(patternsCollector, nbThreads);
		}

		@SuppressWarnings("rawtypes")
		public void setHadoopContext(Context context) {
			if (this.progressWatch != null) {
				this.progressWatch.setHadoopContext(context);
			}
		}

	}

}
