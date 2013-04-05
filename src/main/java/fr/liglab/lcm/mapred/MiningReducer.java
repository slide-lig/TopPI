package fr.liglab.lcm.mapred;

import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.LCM;
import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import fr.liglab.lcm.internals.ConcatenatedDataset;
import fr.liglab.lcm.mapred.groupers.Grouper;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;

public class MiningReducer extends 
	Reducer<IntWritable, TransactionWritable, 
		ItemAndSupportWritable, TransactionWritable> {
	
	protected int minSupport;
	protected PerItemTopKHadoopCollector collector;

	protected int greatestItemID;
	protected Grouper grouper;
	
	@Override
	protected void setup(Context context)
			throws java.io.IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		this.minSupport = conf.getInt(Driver.KEY_MINSUP, 10);
		int topK = conf.getInt(Driver.KEY_DO_TOP_K, -1);
		
		this.collector = new PerItemTopKHadoopCollector(topK, context);
		
		this.greatestItemID = conf.getInt(Driver.KEY_REBASING_MAX_ID, 1);
		this.grouper = Grouper.factory(conf);
	}
	
	protected void reduce(IntWritable gid, 
			java.lang.Iterable<TransactionWritable> transactions, Context context)
			throws java.io.IOException, InterruptedException {
		
		final WritableTransactionsIterator input = new WritableTransactionsIterator(transactions.iterator());
		ConcatenatedDataset dataset = null;
		try {
			dataset = new ConcatenatedDataset(this.minSupport, input);
		} catch (DontExploreThisBranchException e) {
			// with initial support coreItem = MAX_ITEM , this won't happen
			e.printStackTrace();
		}

		context.progress(); // ping master, otherwise long mining tasks get killed
		
		final LCM lcm = new LCM(this.collector);
		final int[] initPattern = dataset.getDiscoveredClosureItems();

		final TIntArrayList starters = new TIntArrayList();
		this.grouper.fillWithGroupItems(starters, gid.get(), this.greatestItemID);
		
		if (initPattern.length > 0 ) {
			starters.removeAll(initPattern);
			this.collector.collect(dataset.getTransactionsCount(), initPattern);
		}
		
		TIntIterator startersIt = starters.iterator();
		
		while (startersIt.hasNext()) {
			int candidate = startersIt.next();
			
			try {
				lcm.lcm(initPattern, dataset, candidate);
			} catch (DontExploreThisBranchException e) {
				
			}
		}
	}
	
	protected void cleanup(Context context) throws java.io.IOException, InterruptedException {
		this.collector.close();
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
