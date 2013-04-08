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
import gnu.trove.set.hash.TIntHashSet;

public class MiningReducerPhase2 extends
		Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, SupportAndTransactionWritable> {
	
	protected final Log logger = LogFactory.getLog(this.getClass());
	
	protected PerItemTopKHadoopCollector collector;
	
	protected int greatestItemID;
	protected Grouper grouper;
	
	@Override
	protected void setup(Context context)
			throws java.io.IOException, InterruptedException {
		
		final Configuration conf = context.getConfiguration();
		
		int topK = conf.getInt(Driver.KEY_DO_TOP_K, -1);
		
		String dumpPath = conf.get(Driver.KEY_DUMP_ON_HEAP_EXN, "");
		if (dumpPath.length() > 0) {
			HeapDumper.basePath = dumpPath;
		}
		
		this.collector = new PerItemTopKHadoopCollector(topK, context, false, true);
		final TIntIntMap knownBounds = DistCache.readKnownBounds(conf);
		this.collector.setKnownBounds(knownBounds);
		
		this.greatestItemID = conf.getInt(Driver.KEY_REBASING_MAX_ID, 1);
		this.grouper = Grouper.factory(conf);
	}
	
	
	protected void reduce(IntWritable gid, 
			java.lang.Iterable<TransactionWritable> transactions, Context context)
			throws java.io.IOException, InterruptedException {
		
		final Dataset dataset = TransactionWritable.buildDataset(context.getConfiguration(), transactions.iterator());
		
		context.progress(); // ping master, otherwise long mining tasks get killed
		
		final TIntHashSet groupItems = new TIntHashSet();
		this.grouper.fillWithGroupItems(groupItems, gid.get(), this.greatestItemID);
		this.collector.setGroup(groupItems);
		
		final LCM lcm = new LCM(this.collector);
		lcm.lcm(dataset);
	}
	
	
	protected void cleanup(Context context) throws java.io.IOException, InterruptedException {
		this.collector.close();
	}
}
