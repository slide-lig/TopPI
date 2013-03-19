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
		computeGroupsFromFreqHeap(heap, frequentWordsCount, context);
	}
	
	protected void computeGroupsFromFreqHeap(TreeSet<ItemAndBigSupport> heap, 
			long frequentWordsCount, Context context) 
			throws IOException, InterruptedException {
		
		int gid=0,rebased=0;
		
		for (ItemAndBigSupport entry : heap) {
			keyW.set(entry.item);
			valueW.set(gid % this.nbGroups, rebased);
			context.write(keyW, valueW);
			gid++;
			rebased++;
		}
	}
}
