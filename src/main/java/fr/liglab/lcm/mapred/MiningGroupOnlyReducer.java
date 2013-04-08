package fr.liglab.lcm.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.lcm.LCM;
import fr.liglab.lcm.internals.Dataset;
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
	
	protected final Log logger = LogFactory.getLog(this.getClass());
	
	protected PerItemTopKHadoopCollector collector;
	
	protected int greatestItemID;
	protected Grouper grouper;
	
	@Override
	protected void setup(Context context)
			throws java.io.IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
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
		
		logger.info("Loading dataset for group "+gid.get());
		
		final TIntSet starters = new TIntHashSet();
		this.grouper.fillWithGroupItems(starters, gid.get(), this.greatestItemID);
		this.collector.setGroup(starters);
		
		Dataset dataset = TransactionWritable.buildDataset(context.getConfiguration(), transactions.iterator());
		
		logger.info("Loaded dataset for group "+gid.get());
		
		context.progress(); // ping master, otherwise long mining tasks get killed
		
		final LCM lcm = new LCM(this.collector);
		lcm.lcm(dataset);
	}
	
	protected void cleanup(Context context) throws java.io.IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		TIntIntMap reverse = DistCache.readReverseRebasing(conf);
		
		this.collector.close(reverse);
	}
}
