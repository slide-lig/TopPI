package fr.liglab.lcm.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.io.PerItemGroupTopKCollector;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import gnu.trove.iterator.TIntObjectIterator;

public class PerItemTopKHadoopCollector extends PerItemGroupTopKCollector {
	
	protected final static int PING_EVERY = 100000;
	protected int collected = 0;
	
	protected final Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, TransactionWritable>.Context context;

	public PerItemTopKHadoopCollector(
			int k,
			Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, TransactionWritable>.Context currentContext) {
		this(k, currentContext, true, true);
	}

	public PerItemTopKHadoopCollector(
			int k,
			Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, TransactionWritable>.Context currentContext,
			boolean mineInGroup, boolean mineOutGroup) {
		super(null, k, mineInGroup, mineOutGroup);
		this.context = currentContext;
	}
	
	@Override
	public void collect(int support, int[] pattern) {
		super.collect(support, pattern);
		
		this.collected++;
		if (this.collected >= PING_EVERY) {
			this.collected = 0;
			this.context.progress();
		}
	}

	@Override
	public long close() {
		final ItemAndSupportWritable keyW = new ItemAndSupportWritable();
		final TransactionWritable valueW = new TransactionWritable();
		long outputted = 0;

		TIntObjectIterator<PatternWithFreq[]> it = this.topK.iterator();

		while (it.hasNext()) {
			it.advance();
			keyW.setItem(it.key());
			final PatternWithFreq[] itemTopK = it.value();

			for (int i = 0; i < itemTopK.length && itemTopK[i] != null; i++) {
				PatternWithFreq entry = itemTopK[i];

				keyW.setSupport(entry.getSupportCount());
				valueW.set(entry.getPattern());

				try {
					context.write(keyW, valueW);
					outputted++;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return outputted;
	}
}
