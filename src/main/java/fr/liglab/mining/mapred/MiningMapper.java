package fr.liglab.mining.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fr.liglab.mining.mapred.writables.ConcatenatedTransactionsWritable;
import fr.liglab.mining.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * outputted records are GroupID => concatenated transactions
 * Transactions will be filtered and rebased according to the global rebasing map 
 */
public final class MiningMapper extends Mapper<LongWritable, Text, IntWritable, ConcatenatedTransactionsWritable> {
	
	// in-mapper combiners
	private TIntObjectMap<List<int[]>> combiner;
	private TIntIntMap combinerConcatenated;
	private final IntWritable keyW = new IntWritable();
	private final ConcatenatedTransactionsWritable valueW = new ConcatenatedTransactionsWritable();
	
	// app parameters
	private int nbGroups;
	private TIntIntMap rebasing = null;
	private Grouper grouper = null;
	private int dumpEvery = Integer.MAX_VALUE;

	// these will be cleared after each map(), they're just here to avoid repeated instanciations
	protected final ItemsetsFactory transaction = new ItemsetsFactory();
	protected final TIntSet destinations = new TIntHashSet();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		if (this.rebasing == null) {
			this.rebasing = DistCache.readRebasing(conf);
			this.nbGroups = conf.getInt(TopLCMoverHadoop.KEY_NBGROUPS, 1);
			int maxItem = conf.getInt(TopLCMoverHadoop.KEY_REBASING_MAX_ID, 1);
			this.grouper = new Grouper(this.nbGroups, maxItem);
			this.dumpEvery = conf.getInt(TopLCMoverHadoop.KEY_COMBINED_TRANS_SIZE, Integer.MAX_VALUE);
		}
		
		this.combiner = new TIntObjectHashMap<List<int[]>>(this.nbGroups);
		this.combinerConcatenated = new TIntIntHashMap(this.nbGroups);
		
		for (int i = 0; i < this.nbGroups; i++) {
			this.combiner.put(i, new ArrayList<int[]>());
			this.combinerConcatenated.put(i, 0);
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] tokens = value.toString().split("\\s+");
		
		for (String token : tokens) {
			int item = Integer.parseInt(token);
			
			if (this.rebasing.containsKey(item)) {
				int rebased = this.rebasing.get(item);
				this.transaction.add(rebased);
				this.destinations.add(this.grouper.getGroupId(rebased));
			}
		}
		
		if (!destinations.isEmpty()) {
			int[] transaction = this.transaction.get();
			
			TIntIterator it = this.destinations.iterator();
			while (it.hasNext()) {
				int gid = it.next();
				
				final List<int[]> concatenated = this.combiner.get(gid);
				concatenated.add(transaction);
				
				final int len = transaction.length;
				int concatenatedLen = this.combinerConcatenated.adjustOrPutValue(gid, len, len);
				
				if (concatenatedLen >= this.dumpEvery) {
					this.keyW.set(gid);
					this.valueW.set(concatenated, concatenatedLen);
					context.write(this.keyW, this.valueW);
					
					this.combiner.put(gid, new ArrayList<int[]>());
					this.combinerConcatenated.put(gid, 0);
				}
			}
			
			this.destinations.clear();
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		TIntObjectIterator<List<int[]>> it = this.combiner.iterator();
		
		while (it.hasNext()) {
			it.advance();
			
			final int gid = it.key();
			final int len = this.combinerConcatenated.get(gid);
			
			if (len > 0) {
				keyW.set(gid);
				valueW.set(it.value(), len);
				context.write(keyW, valueW);
			}
		}
	}
}
