package fr.liglab.lcm.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import fr.liglab.lcm.LCM;
import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.mapred.groupers.Grouper;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.SupportAndTransactionWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import fr.liglab.lcm.util.HeapDumper;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.set.hash.TIntHashSet;

public class MiningTwoPhasesReducer extends
		Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, SupportAndTransactionWritable> {
	
	static final String BOUNDS_OUTPUT_NAME = "bounds";
	static final String KEY_BOUNDS_PATH = "lcm.bound.path";
	static final String KEY_PHASE_ID = "lcm.mining.phase";
	
	protected final Log logger = LogFactory.getLog(this.getClass());
	
	protected PerItemTopKHadoopCollector collector;
	protected int phase;
	
	protected MultipleOutputs<ItemAndSupportWritable,SupportAndTransactionWritable> sideOutputs;
	protected String boundsPath;
	
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
		
		this.phase = conf.getInt(KEY_PHASE_ID, 1);
		
		if (this.phase == 1) {
			this.collector = new PerItemTopKHadoopCollector(topK, context, true, false);
			
			this.sideOutputs = new MultipleOutputs<ItemAndSupportWritable, SupportAndTransactionWritable>(context);
			this.boundsPath = context.getConfiguration().get(KEY_BOUNDS_PATH);
		} else {
			this.collector = new PerItemTopKHadoopCollector(topK, context, false, true);
			TIntIntMap bounds = DistCache.readKnownBounds(conf);
			this.collector.setKnownBounds(bounds);
		}
		
		
		this.greatestItemID = conf.getInt(Driver.KEY_REBASING_MAX_ID, 1);
		this.grouper = Grouper.factory(conf);
	}
	
	
	protected void reduce(IntWritable gid, 
			java.lang.Iterable<TransactionWritable> transactions, Context context)
			throws java.io.IOException, InterruptedException {
		
		final Dataset dataset = TransactionWritable.buildDataset(context.getConfiguration(), transactions.iterator());
		
		context.progress(); // ping master, otherwise long mining tasks get killed
		
		final TIntArrayList starters = new TIntArrayList();
		this.grouper.fillWithGroupItems(starters, gid.get(), this.greatestItemID);
		this.collector.setGroup(new TIntHashSet(starters));
		
		final LCM lcm = new LCM(this.collector);
		final int[] initPattern = dataset.getDiscoveredClosureItems();
		
		if (initPattern.length > 0 ) {
			starters.removeAll(initPattern);
			
			if (this.phase == 1) {
				this.collector.collect(dataset.getTransactionsCount(), initPattern);
			}
		}
		
		TIntIterator startersIt = starters.iterator();
		
		while (startersIt.hasNext()) {
			int candidate = startersIt.next();
			
			try {
				lcm.lcm(initPattern, dataset, candidate);
			} catch (DontExploreThisBranchException e) {
				
			}
		}
		
		if (this.phase == 1) {
			this.dumpBounds();
		}
	}
	
	protected void dumpBounds() throws IOException, InterruptedException {
		final IntWritable itemW  = new IntWritable();
		final IntWritable boundW = new IntWritable();
		
		final TIntIntMap bounds = this.collector.getTopKBounds();
		final TIntIntIterator boundsIter = bounds.iterator();
		
		while (boundsIter.hasNext()) {
			boundsIter.advance();
			itemW.set(boundsIter.key());
			boundW.set(boundsIter.value());
			this.sideOutputs.write(BOUNDS_OUTPUT_NAME, itemW, boundW, this.boundsPath);
		}
	}
	
	protected void cleanup(Context context) throws java.io.IOException, InterruptedException {
		if (this.phase == 1) {
			this.sideOutputs.close();
		}
		
		this.collector.close();
	}
}
