package fr.liglab.lcm.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.io.PerItemGroupTopKCollector;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.SupportAndTransactionWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;

public class PerItemTopKHadoopCollector extends PerItemGroupTopKCollector {
	
	// minimum interval between pokes to job tracker, in milliseconds
	protected final static int PING_AT_MOST_EVERY = 60000;
	private static long latestPing = 0;
	
	protected final Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, SupportAndTransactionWritable>.Context context;

	public PerItemTopKHadoopCollector(
			int k,
			Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, SupportAndTransactionWritable>.Context currentContext) {
		this(k, currentContext, true, true);
	}

	public PerItemTopKHadoopCollector(
			int k,
			Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, SupportAndTransactionWritable>.Context currentContext,
			boolean mineInGroup, boolean mineOutGroup) {
		super(null, k, mineInGroup, mineOutGroup);
		this.context = currentContext;
	}
	
	@Override
	public void collect(int support, int[] pattern) {
		super.collect(support, pattern);
		
		if ( (System.currentTimeMillis()-latestPing) > PING_AT_MOST_EVERY) {
			this.context.progress();
			latestPing = System.currentTimeMillis();
		}
	}

	@Override
	public long close() {
		return this.close(null);
	}
	
	/**
	 * @param reverse rebasing map - may be null, in which case patterns are not rebased
	 */
	public long close(TIntIntMap reverse) {
		final ItemAndSupportWritable keyW = new ItemAndSupportWritable();
		final SupportAndTransactionWritable valueW = new SupportAndTransactionWritable();
		long outputted = 0;
		
		TIntObjectIterator<PatternWithFreq[]> it = this.topK.iterator();
		
		while (it.hasNext()) {
			it.advance();
			
			if (reverse == null) {
				keyW.setItem(it.key());
			} else {
				keyW.setItem(reverse.get(it.key()));
			}
			
			final PatternWithFreq[] itemTopK = it.value();

			for (int i = 0; i < itemTopK.length && itemTopK[i] != null; i++) {
				PatternWithFreq entry = itemTopK[i];
				
				int support = entry.getSupportCount();
				int[] pattern = entry.getPattern();
				
				if (reverse != null) {
					for (int j = 0; j < pattern.length; j++) {
						pattern[j] = reverse.get(pattern[j]);
					}
				}
				
				keyW.setSupport(support);
				valueW.set(support, pattern);

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
