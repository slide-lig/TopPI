package fr.liglab.lcm.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.PLCM;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.mapred.groupers.Grouper;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.SupportAndTransactionWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import fr.liglab.lcm.util.FakeExtensionsIterator;
import gnu.trove.list.array.TIntArrayList;

public class MiningReducer extends 
	Reducer<IntWritable, TransactionWritable, 
	ItemAndSupportWritable, SupportAndTransactionWritable> {
	
	protected PerItemTopKHadoopCollector collector;

	protected int greatestItemID;
	protected Grouper grouper;
	protected int topK;
	protected int nbThreads;
	
	@Override
	protected void setup(Context context)
			throws java.io.IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		topK = conf.getInt(Driver.KEY_DO_TOP_K, -1);
		
		this.greatestItemID = conf.getInt(Driver.KEY_REBASING_MAX_ID, 1);
		this.grouper = Grouper.factory(conf);
		
		this.nbThreads = conf.getInt(Driver.KEY_NB_THREADS, 1);
	}
	
	protected void reduce(IntWritable gid, 
			java.lang.Iterable<TransactionWritable> transactions, Context context)
			throws java.io.IOException, InterruptedException {
		
		Dataset dataset = TransactionWritable.buildDataset(context.getConfiguration(), transactions.iterator());

		context.progress(); // ping master, otherwise long mining tasks get killed

		this.collector = new PerItemTopKHadoopCollector(this.topK, dataset, context);
		
		final PLCM lcm = new PLCM(this.collector, this.nbThreads);

		final TIntArrayList starters = new TIntArrayList();
		this.grouper.fillWithGroupItems(starters, gid.get(), this.greatestItemID);
		
		FakeExtensionsIterator fake = new FakeExtensionsIterator(
				dataset.getCandidatesIterator().getSortedFrequents(), 
				starters.iterator()
			);
		
		lcm.lcm(dataset, fake);
		
		this.collector.close();
	}
}
