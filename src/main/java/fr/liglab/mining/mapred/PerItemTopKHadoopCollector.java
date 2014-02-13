package fr.liglab.mining.mapred;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import fr.liglab.mining.internals.FrequentsIterator;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.map.TIntIntMap;

@SuppressWarnings("rawtypes")
class PerItemTopKHadoopCollector extends PerItemTopKCollector {
	
	private int nbCollected = 0;
	private long lengthCollected = 0;
	
	private final Context context;
	private final IntWritable keyW = new IntWritable();
	private final SupportAndTransactionWritable valueW = new SupportAndTransactionWritable();
	
	public PerItemTopKHadoopCollector(Context c, final int k, final int nbItems, final FrequentsIterator items) {
		super(null, k, nbItems, items);
		this.context = c;
	}

	@Override
	@SuppressWarnings("unchecked")
	public long close() {
		
		TIntObjectIterator<PatternWithFreq[]> entries = this.topK.iterator();
		
		while (entries.hasNext()) {
			entries.advance();
			this.keyW.set(entries.key());
			final PatternWithFreq[] itemTopK = entries.value();
			
			for (int i = 0; i < itemTopK.length; i++) {
				if (itemTopK[i] == null) {
					break;
				} else {
					final int[] pattern = itemTopK[i].getPattern();
					
					if (pattern == null) {
						break;
					}
					
					this.nbCollected++;
					this.lengthCollected += pattern.length;
					this.valueW.set(itemTopK[i].getSupportCount(), pattern);
					
					try {
						this.context.write(this.keyW, this.valueW);
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		return this.nbCollected;
	}
	
	@Override
	public int getAveragePatternLength() {
		return (int)(this.lengthCollected / this.nbCollected);
	}
	
	public void writeTopKBounds(MultipleOutputs<?, ?> sideOutputs, String outputName, String path, int minsup) throws IOException, InterruptedException {
		final IntWritable itemW  = new IntWritable();
		final IntWritable boundW = new IntWritable();
		
		TIntObjectIterator<PatternWithFreq[]> it = this.topK.iterator();
		
		while (it.hasNext()) {
			it.advance();
			if (it.value()[this.k - 1] != null) {
				final int supportCount = it.value()[this.k - 1].getSupportCount();
				
				if (supportCount > minsup) {
					itemW.set(it.key());
					boundW.set(supportCount);
					sideOutputs.write(outputName, itemW, boundW, path);
				}
			}
		}
	}

	// note that "close" overloaded by this class takes into account potentially-null patterns
	public void preloadBounds(TIntIntMap perItemBounds) {
		TIntIntIterator iterator = perItemBounds.iterator();
		while (iterator.hasNext()) {
			iterator.advance();
			
			final int item = iterator.key();
			PatternWithFreq[] top = this.topK.get(item);
			
			if (top != null) {
				Arrays.fill(top, new PatternWithFreq(iterator.value(), item));
			}
		}
	}
}
