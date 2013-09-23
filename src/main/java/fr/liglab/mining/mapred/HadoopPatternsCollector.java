package fr.liglab.mining.mapred;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer.Context;

import fr.liglab.mining.io.PatternsCollector;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;

@SuppressWarnings("rawtypes")
public class HadoopPatternsCollector implements PatternsCollector {
	
	private int nbCollected = 0;
	private long lengthCollected = 0;
	
	private final Context context;
	private final NullWritable keyW = NullWritable.get();
	private final SupportAndTransactionWritable valueW;
	
	public HadoopPatternsCollector(Context c) {
		this.context = c;
		this.valueW = new SupportAndTransactionWritable();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void collect(int support, int[] pattern) {
		this.nbCollected++;
		this.lengthCollected += pattern.length;
		
		this.valueW.set(support, pattern);
		try {
			this.context.write(this.keyW, this.valueW);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public long close() {
		return this.nbCollected;
	}

	@Override
	public int getAveragePatternLength() {
		return (int)(this.lengthCollected / this.nbCollected);
	}

}
