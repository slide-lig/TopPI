package fr.liglab.lcm.mapred;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import gnu.trove.map.TIntIntMap;

public class AggregationReducer extends Reducer<ItemAndSupportWritable, TransactionWritable, ItemAndSupportWritable, TransactionWritable> {
	
	protected final ItemAndSupportWritable keyW = new ItemAndSupportWritable();
	protected final TransactionWritable valueW = new TransactionWritable();
	
	protected TIntIntMap reverseBase = null;
	
	protected int k,lastItem,lastCount;
	
	@Override
	protected void setup(Context context) throws java.io.IOException , InterruptedException {
		
		if (this.reverseBase == null) {
			Configuration conf = context.getConfiguration();
			
			this.k = conf.getInt(Driver.KEY_DO_TOP_K, -1);
			this.reverseBase = DistCache.readReverseRebasing(conf);
		}
		
		this.lastItem = -1;
		this.lastCount = 0;
	}
	
	protected void reduce(ItemAndSupportWritable key, java.lang.Iterable<TransactionWritable> patterns, Context context)
			throws java.io.IOException, InterruptedException {
		
		if (key.getItem() != this.lastItem) {
			this.lastCount = 0;
			this.lastItem = key.getItem();
			
			int rebased = this.reverseBase.get(this.lastItem);
			this.keyW.setItem(rebased);
		}
		
		if (this.lastCount < this.k) {
			this.keyW.setSupport(key.getSupport());
			
			Iterator<TransactionWritable> it = patterns.iterator();
			while (it.hasNext() && this.lastCount < this.k) {
				int[] transaction = it.next().get();
				
				for (int i = 0; i < transaction.length; i++) {
					transaction[i] = transaction[i];
				}
				
				this.valueW.set(transaction);
				context.write(this.keyW, this.valueW);
				this.lastCount++;
			}
		}
	}
	
}
