package fr.liglab.lcm.mapred;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.io.PerItemTopKCollectorThreadSafeInitialized;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.SupportAndTransactionWritable;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;

public class PerItemTopKHadoopCollector extends PerItemTopKCollectorThreadSafeInitialized {
	
	// minimum interval between pokes to job tracker, in milliseconds
	protected final static int PING_AT_MOST_EVERY = 60000;
	private static AtomicLong latestPing = new AtomicLong(0);
	
	protected final Reducer<?, ?, ItemAndSupportWritable, SupportAndTransactionWritable>.Context context;

	public PerItemTopKHadoopCollector(
			int k, Dataset dataset,
			Reducer<?, ?, ItemAndSupportWritable, SupportAndTransactionWritable>.Context currentContext) {
		this(k, currentContext, dataset, true, true);
	}

	public PerItemTopKHadoopCollector(
			int k,
			Reducer<?, ?, ItemAndSupportWritable, SupportAndTransactionWritable>.Context currentContext,
			Dataset dataset,
			boolean mineInGroup, boolean mineOutGroup) {
		super(null, k, dataset, mineInGroup, mineOutGroup);
		this.context = currentContext;
	}
	
	@Override
	public void collect(int support, int[] pattern) {
		super.collect(support, pattern);
		
		if ( (System.currentTimeMillis()-latestPing.get()) > PING_AT_MOST_EVERY) {
			latestPing.set(System.currentTimeMillis());
			this.context.progress();
		}
	}

	@Override
	public long close() {
		return this.close(null);
	}
	
	/**
	 * for debugging only !
	 */
	@Deprecated
	public TIntObjectMap<PatternWithFreq[]> getTopK() {
		return this.topK;
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
	
	/*
	 * DEBUG ONLY
	 */
	@Deprecated
	public void init(int i, int[] supports) {
		PatternWithFreq[] patterns = new PatternWithFreq[k];
		
		for (int j = 0; j < supports.length; j++) {
			patterns[j] = new PatternWithFreq(supports[j], new int[0]);
		}
		
		this.topK.put(i, patterns);
	}
}
