package fr.liglab.lcm.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.io.PerItemTopKCollector;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.TIntSet;

public class PerItemTopKHadoopCollector extends PerItemTopKCollector {

	protected static final int PING_EVERY = 1000;
	protected int collected;

	protected final boolean mineInGroup;
	protected final boolean mineOutGroup;
	protected TIntSet group;

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
		super(null, k, false);
		this.mineInGroup = mineInGroup;
		this.mineOutGroup = mineOutGroup;
		this.context = currentContext;
		this.collected = 0;
	}

	@Override
	protected void insertPatternInTop(int support, int[] pattern, int item) {
		if (!this.mineInGroup && this.group.contains(item)) {
			return;
		}
		if (!this.mineOutGroup && !this.group.contains(item)) {
			return;
		}
		super.insertPatternInTop(support, pattern, item);
	}

	@Override
	protected int checkExploreOtherItem(int item, int extension,
			int extensionSupport, TIntIntMap supportCounts,
			TIntIntMap failedPPTests) {
		if (!this.mineInGroup && this.group.contains(item)) {
			return Integer.MAX_VALUE;
		}
		if (!this.mineOutGroup && !this.group.contains(item)) {
			return Integer.MAX_VALUE;
		}
		return super.checkExploreOtherItem(item, extension, extensionSupport,
				supportCounts, failedPPTests);
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

	@Override
	public TIntIntMap getTopKBounds() {
		if (this.mineInGroup && this.mineOutGroup) {
			return super.getTopKBounds();
		} else if (this.mineInGroup) {
			final TIntIntHashMap bounds = new TIntIntHashMap(this.topK.size());
			TIntIterator it = this.group.iterator();
			while (it.hasNext()) {
				int item = it.next();
				int bound = 0;
				PatternWithFreq[] itemTop = this.topK.get(item);
				if (itemTop != null) {
					if (itemTop[this.k - 1] != null) {
						bound = itemTop[this.k - 1].getSupportCount();
					}
					bounds.put(item, bound);
				}
			}
			return bounds;
		} else if (this.mineOutGroup) {
			final TIntIntHashMap bounds = new TIntIntHashMap(this.topK.size());
			TIntIterator it = this.topK.keySet().iterator();
			while (it.hasNext()) {
				int item = it.next();
				if (!this.group.contains(item)) {
					int bound = 0;
					PatternWithFreq[] itemTop = this.topK.get(item);
					if (itemTop[this.k - 1] != null) {
						bound = itemTop[this.k - 1].getSupportCount();
					}
					bounds.put(item, bound);
				}
			}
			return bounds;
		} else {
			return new TIntIntHashMap();
		}
	}

}
