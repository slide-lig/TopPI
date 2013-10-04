package fr.liglab.mining.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;

import fr.liglab.mining.internals.FrequentsIterator;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;
import gnu.trove.iterator.TIntObjectIterator;

@SuppressWarnings("rawtypes")
public class PerItemTopKHadoopCollector extends PerItemTopKCollector {
	
	private int nbCollected = 0;
	private long lengthCollected = 0;
	
	private final Context context;
	private final IntWritable keyW = new IntWritable();
	private final SupportAndTransactionWritable valueW = new SupportAndTransactionWritable();
	
	public PerItemTopKHadoopCollector(Context c, final int k, final int nbItems,
			final FrequentsIterator items) {
		
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
}
