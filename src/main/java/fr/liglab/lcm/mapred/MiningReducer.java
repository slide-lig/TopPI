package fr.liglab.lcm.mapred;

import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.LCM;
import fr.liglab.lcm.internals.ConcatenatedDataset;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.io.PatternsCollector;
import fr.liglab.lcm.mapred.writables.SupportAndTransactionWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;

public class MiningReducer extends 
	Reducer<IntWritable, TransactionWritable, 
		IntWritable, SupportAndTransactionWritable> {
	
	
	protected int minSupport;
	protected int topK;
	
	@Override
	protected void setup(Context context)
			throws java.io.IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		this.minSupport = conf.getInt(Driver.KEY_MINSUP, 10);
		this.topK = conf.getInt(Driver.KEY_DO_TOP_K, -1);
	}
	
	protected void reduce(IntWritable gid, 
			java.lang.Iterable<TransactionWritable> transactions, Context context)
			throws java.io.IOException, InterruptedException {
		
		final Configuration conf = context.getConfiguration();
		final TIntArrayList starters = DistCache.readStartersFor(conf, gid.get());
		
		PatternsCollector collector;
		if (this.topK > 0) {
			collector = new PerItemTopKHadoopCollector(this.topK, context);
		} else {
			collector = new HadoopCollector(context);
		}
		
		final WritableTransactionsIterator input = new WritableTransactionsIterator(transactions.iterator());
		final Dataset dataset = new ConcatenatedDataset(this.minSupport, input);
		
		final LCM lcm = new LCM(collector);
		final int[] emptyPattern = new int[0];
		
		starters.sort();
		TIntIterator startersIt = starters.iterator();
		
		while (startersIt.hasNext()) {
			lcm.lcm(emptyPattern, dataset, startersIt.next());
		}
		
		collector.close();
	}
	
	
	
	public static final class WritableTransactionsIterator implements Iterator<int[]> {
		
		private final Iterator<TransactionWritable> wrapped;
		
		public WritableTransactionsIterator(Iterator<TransactionWritable> original) {
			this.wrapped = original;
		}
		
		public boolean hasNext() {
			return this.wrapped.hasNext();
		}

		public int[] next() {
			return this.wrapped.next().get();
		}

		public void remove() {
			throw new NotImplementedException();
		}
	}
}
