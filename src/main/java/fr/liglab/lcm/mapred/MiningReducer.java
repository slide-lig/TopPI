package fr.liglab.lcm.mapred;

import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.LCM;
import fr.liglab.lcm.internals.ConcatenatedDataset;
import fr.liglab.lcm.io.PatternsCollector;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;

public class MiningReducer extends 
	Reducer<IntWritable, TransactionWritable, 
		ItemAndSupportWritable, TransactionWritable> {
	
	protected int minSupport;
	protected PatternsCollector collector;
	
	@Override
	protected void setup(Context context)
			throws java.io.IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		this.minSupport = conf.getInt(Driver.KEY_MINSUP, 10);
		int topK = conf.getInt(Driver.KEY_DO_TOP_K, -1);
		
		if (topK > 0) {
			this.collector = new PerItemTopKHadoopCollector(topK, context);
		} else {
			this.collector = new HadoopCollector(context);
		}
	}
	
	protected void reduce(IntWritable gid, 
			java.lang.Iterable<TransactionWritable> transactions, Context context)
			throws java.io.IOException, InterruptedException {
		
		final Configuration conf = context.getConfiguration();
		final TIntArrayList starters = DistCache.readStartersFor(conf, gid.get());
		
		System.out.println("Hello from MiningReducer #"+gid.get()+" - s="+this.minSupport+" k="+conf.getInt(Driver.KEY_DO_TOP_K, -1));
		
		final WritableTransactionsIterator input = new WritableTransactionsIterator(transactions.iterator());
		final ConcatenatedDataset dataset = new ConcatenatedDataset(this.minSupport, input);

		context.progress(); // ping master, otherwise long mining tasks get killed
		
		final LCM lcm = new LCM(this.collector);
		final int[] initPattern = dataset.getDiscoveredClosureItems();
		
		starters.removeAll(initPattern);
		starters.sort();
		TIntIterator startersIt = starters.iterator();
		System.out.println("#" + gid.get()+" Starting mining over "+starters.toString());
		while (startersIt.hasNext()) {
			int candidate = startersIt.next();
			
			if (dataset.prefixPreservingTest(candidate)) {
				lcm.lcm(initPattern, dataset, candidate);
			}
		}
		System.out.println("#" + gid.get()+" done - "+lcm.toString());
	}
	
	protected void cleanup(Context context) throws java.io.IOException, InterruptedException {
		System.out.println("closing...");
		this.collector.close();
		System.out.println("... closed.");
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
