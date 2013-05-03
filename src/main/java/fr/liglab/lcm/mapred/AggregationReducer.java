package fr.liglab.lcm.mapred;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.SupportAndTransactionWritable;
import gnu.trove.map.TIntIntMap;

public class AggregationReducer extends
		Reducer<ItemAndSupportWritable, SupportAndTransactionWritable, IntWritable, SupportAndTransactionWritable> {

	protected final IntWritable keyW = new IntWritable();
	protected final SupportAndTransactionWritable valueW = new SupportAndTransactionWritable();

	protected TIntIntMap reverseBase = null;

	protected int k;

	@Override
	protected void setup(Context context) throws java.io.IOException, InterruptedException {

		if (this.reverseBase == null) {
			Configuration conf = context.getConfiguration();

			this.k = conf.getInt(Driver.KEY_DO_TOP_K, -1);
			this.reverseBase = DistCache.readReverseRebasing(conf);
		}
	}

	protected void reduce(ItemAndSupportWritable key, Iterable<SupportAndTransactionWritable> patterns, Context context)
			throws java.io.IOException, InterruptedException {

		int rebased = this.reverseBase.get(key.getItem());
		this.keyW.set(rebased);
		Iterator<SupportAndTransactionWritable> iter = patterns.iterator();
		for (int count = 0; count < this.k && iter.hasNext(); count++) {
			SupportAndTransactionWritable entry = iter.next();
			int[] transaction = entry.getTransaction();
			for (int i = 0; i < transaction.length; i++) {
				transaction[i] = this.reverseBase.get(transaction[i]);
			}
			this.valueW.set(entry.getSupport(), transaction);
			context.write(this.keyW, this.valueW);
		}
	}

}
