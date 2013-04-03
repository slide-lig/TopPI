package fr.liglab.lcm.mapred;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.mapred.writables.GIDandRebaseWritable;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.util.ItemAndBigSupport;
import gnu.trove.iterator.TIntLongIterator;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.hash.TIntLongHashMap;

public class ItemGroupingReducer extends
		Reducer<NullWritable, ItemAndSupportWritable, IntWritable, GIDandRebaseWritable> {
	
	protected TIntLongMap itemSupports;
	protected int minSupport;
	protected int nbGroups;
	
	protected final IntWritable keyW = new IntWritable();
	protected final GIDandRebaseWritable valueW = new GIDandRebaseWritable();
	
	@Override
	protected void setup(Context context) throws java.io.IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		this.itemSupports = new TIntLongHashMap();
		this.minSupport = conf.getInt(Driver.KEY_MINSUP, 10);
		this.nbGroups = conf.getInt(Driver.KEY_NBGROUPS, 50);
	}
	
	@Override
	protected void reduce(NullWritable key, 
			Iterable<ItemAndSupportWritable> values, Context context)
			throws java.io.IOException, InterruptedException {
		
		Iterator<ItemAndSupportWritable> it = values.iterator();
		
		while (it.hasNext()) {
			ItemAndSupportWritable entry = it.next();
			long sup = entry.getSupport();
			
			this.itemSupports.adjustOrPutValue(entry.getItem(), sup, sup);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws java.io.IOException, InterruptedException {
		final TreeSet<ItemAndBigSupport> heap = new TreeSet<ItemAndBigSupport>();
		long frequentWordsCount = 0;
		
		TIntLongIterator it = this.itemSupports.iterator();
		while(it.hasNext()) {
			it.advance();
			long support = it.value();
			if (support >= this.minSupport) {
				heap.add(new ItemAndBigSupport(it.key(), support));
				frequentWordsCount += support;
			}
		}
		
		this.itemSupports = null;
		
		Configuration conf = context.getConfiguration();
		int singleGroupId = conf.getInt(Driver.KEY_SINGLE_GROUP_ID, -1);
		if (singleGroupId >= 0) {
			computeSingleGroupFromFreqHeap(heap, singleGroupId, frequentWordsCount, context);
		} else {
			computeGroupsFromFreqHeap(heap, frequentWordsCount, context);
		}
	}
	
	protected void computeGroupsFromFreqHeap(TreeSet<ItemAndBigSupport> heap, 
			long frequentWordsCount, Context context) 
			throws IOException, InterruptedException {
		
		int rebased = 0;
		
		for (ItemAndBigSupport entry : heap) {
			keyW.set(entry.item);
			valueW.set(rebased % this.nbGroups, rebased);
			context.write(keyW, valueW);
			rebased++;
		}
	}
	
	/**
	 * When Driver.KEY_SINGLE_GROUP_ID is set, it will be the only group generated and mined.
	 * All other item will have GID=-1 which means "they're frequent but won't be copied anywhere"
	 */
	protected void computeSingleGroupFromFreqHeap(TreeSet<ItemAndBigSupport> heap, 
			int singleGroupId, long frequentWordsCount, Context context) 
			throws IOException, InterruptedException {
		
		int rebased = 0;
		
		for (ItemAndBigSupport entry : heap) {
			keyW.set(entry.item);
			
			int gid = rebased % this.nbGroups;
			if (gid != singleGroupId) {
				gid = -1;
			}
			
			valueW.set(gid, rebased);
			context.write(keyW, valueW);
			rebased++;
		}
	}
}
