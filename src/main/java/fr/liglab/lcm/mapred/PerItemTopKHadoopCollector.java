package fr.liglab.lcm.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.io.PerItemTopKCollector;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import gnu.trove.iterator.TIntObjectIterator;

public class PerItemTopKHadoopCollector extends PerItemTopKCollector {
	
	protected static final int PING_EVERY = 1000;
	protected int collected;
	
	protected final Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, TransactionWritable>.Context context;
	
	public PerItemTopKHadoopCollector(int k, Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, TransactionWritable>.Context currentContext) {
		super(null, k);
		
		this.context = currentContext;
		this.collected = 0;
	}
	
	@Override
	public void collect(int support, int[] pattern) {
		if (this.collected == PING_EVERY) {
			this.collected = 0;
			this.context.progress();
		} else {
			this.collected++;
		}
		
		super.collect(support, pattern);
	}
	
	@Override
	public long close() {
		final ItemAndSupportWritable keyW = new ItemAndSupportWritable();
		final TransactionWritable valueW = new TransactionWritable();
		long outputted = 0;
		
		TIntObjectIterator<PatternWithFreq[]> it = this.topK.iterator();
		
		while(it.hasNext()) {
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
