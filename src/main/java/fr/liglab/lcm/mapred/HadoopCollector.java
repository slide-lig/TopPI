package fr.liglab.lcm.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.io.PatternsCollector;
import fr.liglab.lcm.mapred.writables.SupportAndTransactionWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import gnu.trove.map.TIntIntMap;

public class HadoopCollector extends PatternsCollector {

	protected final Reducer<IntWritable, TransactionWritable, IntWritable, SupportAndTransactionWritable>.Context context;
	protected final IntWritable keyW = new IntWritable();
	protected final SupportAndTransactionWritable valueW = new SupportAndTransactionWritable();

	protected boolean error = false;

	public HadoopCollector(
			Reducer<IntWritable, TransactionWritable, IntWritable, SupportAndTransactionWritable>.Context currentContext) {
		this.context = currentContext;
	}

	@Override
	public int explore(int[] currentPattern, int extension,
			int[] sortedFreqItems, TIntIntMap supportCounts,
			final int previousItem, final int resultForPreviousItem) {

		return -1;
	}

	@Override
	public void collect(int support, int[] pattern) {

		this.valueW.set(support, pattern);

		for (int item : pattern) {
			this.keyW.set(item);
			try {
				this.context.write(this.keyW, this.valueW);

			} catch (IOException e) {
				e.printStackTrace();
				error = true;
			} catch (InterruptedException e) {
				e.printStackTrace();
				error = true;
			}
		}
	}

	@Override
	public void close() {

	}
}
