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
import fr.liglab.lcm.mapred.writables.SupportAndTransactionWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import fr.liglab.lcm.util.HeapDumper;
import gnu.trove.map.TIntIntMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

public class MiningGroupOnlyReducer extends 
	Reducer<IntWritable, TransactionWritable, 
		ItemAndSupportWritable, SupportAndTransactionWritable> {
	
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
		
		String dumpPath = conf.get(Driver.KEY_DUMP_ON_HEAP_EXN, "");
		if (dumpPath.length() > 0) {
			HeapDumper.basePath = dumpPath;
		}
		
		this.collector = new PerItemTopKHadoopCollector(topK, context, true, false);
		
		this.greatestItemID = conf.getInt(Driver.KEY_REBASING_MAX_ID, 1);
		this.grouper = Grouper.factory(conf);
	}
	
	protected void reduce(IntWritable gid, 
			java.lang.Iterable<TransactionWritable> transactions, Context context)
			throws java.io.IOException, InterruptedException {
		
		final TIntSet starters = new TIntHashSet();
		this.grouper.fillWithGroupItems(starters, gid.get(), this.greatestItemID);
		this.collector.setGroup(starters);
		
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
		lcm.lcm(dataset);
	}
	
	protected void cleanup(Context context) throws java.io.IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		TIntIntMap reverse = DistCache.readReverseRebasing(conf);
		
		this.collector.close(reverse);
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
