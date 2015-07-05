package fr.liglab.mining.mapred;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.hyptest.ItemsetSupports;
import fr.liglab.hyptest.PatternsHeap;
import fr.liglab.hyptest.TreeMatcher;
import fr.liglab.hyptest.TreeMatcher.NotMonitoredException;
import fr.liglab.mining.mapred.writables.ItemAndSupportWritable;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;
import fr.liglab.mining.util.ProgressWatcherThread;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * As getting the patterns' global support requires a pass on the initial dataset,
 * it is done only once in cleanup()
 */
public class AggregationByCorrelationReducer extends Reducer<ItemAndSupportWritable, SupportAndTransactionWritable, IntWritable, SupportAndTransactionWritable> {
	
	protected int k;
	protected int currentItem;
	protected TIntObjectMap<TreeMatcher> perItempatterns;
	protected TreeMatcher searchedPatterns;
	protected TIntIntMap itemSupports;
	
	@Override
	protected void setup(Context context) throws java.io.IOException , InterruptedException {
		this.k = context.getConfiguration().getInt(TopPIoverHadoop.KEY_CORRELATION_RESULTS, 1);
		this.perItempatterns = new TIntObjectHashMap<TreeMatcher>();
		this.itemSupports = new TIntIntHashMap();
		this.searchedPatterns = new TreeMatcher();
	}
	
	protected void reduce(ItemAndSupportWritable key, java.lang.Iterable<SupportAndTransactionWritable> patterns, Context context)
			throws java.io.IOException, InterruptedException {
		
		TreeMatcher prefixTree = new TreeMatcher();
		int itemSupport = 0;
		int keyItem = key.getItem();
		
		Iterator<SupportAndTransactionWritable> it = patterns.iterator();
		while (it.hasNext()) {
			SupportAndTransactionWritable pattern = it.next();
			int[] itemset = pattern.getTransaction();
			Arrays.sort(itemset);
			prefixTree.addPattern(itemset, pattern.getSupport(), keyItem);
			this.searchedPatterns.addPattern(itemset, -1, keyItem);
			itemSupport = Math.max(itemSupport, pattern.getSupport());
		}
		
		this.perItempatterns.put(keyItem, prefixTree);
		this.itemSupports.put(keyItem, itemSupport);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		final IntWritable keyW = new IntWritable();
		
		ProgressWatcherThread pokeMaster = new ProgressWatcherThread();
		pokeMaster.setHadoopContext(context);
		pokeMaster.start();
		
		getGlobalSupports(getDatasetPath(context));
		
		TIntObjectIterator<TreeMatcher> iterator = this.perItempatterns.iterator();
		while(iterator.hasNext()) {
			iterator.advance();
			int keyItem = iterator.key();
			keyW.set(keyItem);
			TreeMatcher found = iterator.value();
			
			PatternsHeap heap = new PatternsHeap(this.k, this.itemSupports.get(keyItem));
			Iterator<ItemsetSupports> patterns = found.iterator();
			while (patterns.hasNext()) {
				ItemsetSupports pattern = patterns.next();
				int[] itemset = pattern.getItemset();
				try {
					int itemsetSupport = this.searchedPatterns.getSupport(itemset);
					heap.insert(itemset, itemsetSupport, pattern.getSupport());
				} catch (NotMonitoredException e) {
					e.printStackTrace();
				}
			}
			
			Iterator<SupportAndTransactionWritable> writables = heap.getWritables();
			while (writables.hasNext()) {
				context.write(keyW, writables.next());
			}
		}
		
		pokeMaster.interrupt();
	}
	
	private void getGlobalSupports(URI datasetPath) throws IOException {
		final int[] buffer = new int[1024*1024];
		BufferedReader br = new BufferedReader(new FileReader(new File(datasetPath)));
		String line;
		int nbLines = 0;
		while ((line = br.readLine()) != null) {
			String[] splitted = line.split(" ");
			final int length = splitted.length;
			if (length > 0) {
				nbLines++;
			}
			for (int i = 0; i < length; i++) {
				buffer[i] = Integer.parseInt(splitted[i]);
			}
			Arrays.sort(buffer, 0, length);
			this.searchedPatterns.match(buffer, length);
		}
		br.close();
		PatternsHeap.nbTransactions = nbLines;
	}
	
	private URI getDatasetPath(Context context) {
		String marker = context.getConfiguration().get(TopPIoverHadoop.KEY_INPUT);
		String[] sp = marker.split("/");
		if (sp.length > 2) {
			marker = sp[sp.length - 1];
		}
		List<URI> cached = new ArrayList<URI>(1);
		for (URI path : DistCache.getCachedFiles(context)) {
			if (path.toString().contains(marker)) {
				cached.add(path);
			}
		}
		if (cached.size() > 1) {
			throw new RuntimeException("Without 'many items mode' a single input file is expected in the distcache");
		}
		return cached.get(0);
	}
	
	/**
	 * All patterns involving an item should end to the same reducer
	 */
	public static class AggregationPartitioner extends Partitioner<ItemAndSupportWritable, SupportAndTransactionWritable> {
		
		@Override
		public int getPartition(ItemAndSupportWritable key, SupportAndTransactionWritable value, int numPartitions) {
			return key.getItem() % numPartitions;
		}
	}
}
