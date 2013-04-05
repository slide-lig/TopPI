package fr.liglab.lcm.mapred;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.util.ItemAndBigSupport;
import gnu.trove.iterator.TIntLongIterator;
import gnu.trove.map.TIntLongMap;
import gnu.trove.map.hash.TIntLongHashMap;

/**
 * Actually this reducer creates the rebasing map
 */
public class ItemCountingReducer extends
		Reducer<NullWritable, ItemAndSupportWritable, IntWritable, IntWritable> {
	
	public static final String COUNTERS_GROUP = "ItemCounters";
	
	public static final String COUNTER_REBASING_MAX_ID = "rebasing-maxId";
	
	protected TIntLongMap itemSupports;
	protected int minSupport;
	
	protected final IntWritable keyW = new IntWritable();
	protected final IntWritable valueW = new IntWritable();
	
	@Override
	protected void setup(Context context) throws java.io.IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		this.itemSupports = new TIntLongHashMap();
		this.minSupport = conf.getInt(Driver.KEY_MINSUP, 10);
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
		
		TIntLongIterator it = this.itemSupports.iterator();
		while(it.hasNext()) {
			it.advance();
			long support = it.value();
			if (support >= this.minSupport) {
				heap.add(new ItemAndBigSupport(it.key(), support));
			}
		}
		
		this.itemSupports = null;
		int rebased = 0;
		
		for (ItemAndBigSupport entry : heap) {
			this.keyW.set(entry.item);
			this.valueW.set(rebased++);
			context.write(this.keyW, this.valueW);
		}
		
		rebased -= 1;
		
		context.getCounter(COUNTERS_GROUP, COUNTER_REBASING_MAX_ID).setValue(rebased);
	}
}
