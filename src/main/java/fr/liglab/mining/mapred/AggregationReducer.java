package fr.liglab.mining.mapred;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.mining.mapred.writables.ItemAndSupportWritable;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;

public class AggregationReducer extends Reducer<ItemAndSupportWritable, SupportAndTransactionWritable, IntWritable, SupportAndTransactionWritable> {
	
	protected final IntWritable keyW = new IntWritable();
	protected final SupportAndTransactionWritable valueW = new SupportAndTransactionWritable();
	
	protected int k,lastItem,lastCount;
	
	@Override
	protected void setup(Context context) throws java.io.IOException , InterruptedException {
		this.k = context.getConfiguration().getInt(TopLCMoverHadoop.KEY_K, 1);
		this.lastItem = -1;
		this.lastCount = 0;
	}
	
	protected void reduce(ItemAndSupportWritable key, java.lang.Iterable<SupportAndTransactionWritable> patterns, Context context)
			throws java.io.IOException, InterruptedException {
		
		if (key.getItem() != this.lastItem) {
			this.lastCount = 0;
			this.keyW.set(key.getItem());
		}
		
		if (this.lastCount < this.k) {
			Iterator<SupportAndTransactionWritable> it = patterns.iterator();
			while (it.hasNext() && this.lastCount < this.k) {
				context.write(this.keyW, it.next());
				this.lastCount++;
			}
		}
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
