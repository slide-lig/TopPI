package fr.liglab.lcm.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import fr.liglab.lcm.PLCM;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.mapred.groupers.Grouper;
import fr.liglab.lcm.mapred.writables.ItemAndSupportWritable;
import fr.liglab.lcm.mapred.writables.SupportAndTransactionWritable;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import fr.liglab.lcm.util.FakeExtensionsIterator;
import fr.liglab.lcm.util.HeapDumper;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.set.hash.TIntHashSet;

public class MiningTwoPhasesReducer extends
		Reducer<IntWritable, TransactionWritable, ItemAndSupportWritable, SupportAndTransactionWritable> {
	
	static final String BOUNDS_OUTPUT_NAME = "bounds";
	static final String KEY_BOUNDS_PATH = "lcm.bound.path";
	static final String KEY_PHASE_ID = "lcm.mining.phase";
	static final String COUNTER_GROUP = "MiningTwoPhasesReducer";
	static final String COUNTER_BOUNDS_COUNT = "FoundBounds";
	
	protected final Log logger = LogFactory.getLog(this.getClass());
	
	protected PerItemTopKHadoopCollector collector;
	protected int phase;
	protected int topK;
	protected int nbThreads;
	
	protected MultipleOutputs<ItemAndSupportWritable,SupportAndTransactionWritable> sideOutputs;
	protected String boundsPath;
	protected TIntIntMap bounds;
	
	protected int greatestItemID;
	protected Grouper grouper;
	
	@Override
	protected void setup(Context context)
			throws java.io.IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		this.topK = conf.getInt(Driver.KEY_DO_TOP_K, -1);
		
		String dumpPath = conf.get(Driver.KEY_DUMP_ON_HEAP_EXN, "");
		if (dumpPath.length() > 0) {
			HeapDumper.basePath = dumpPath;
		}
		
		this.phase = conf.getInt(KEY_PHASE_ID, 1);
		
		if (this.phase == 1) {
			this.sideOutputs = new MultipleOutputs<ItemAndSupportWritable, SupportAndTransactionWritable>(context);
			this.boundsPath = context.getConfiguration().get(KEY_BOUNDS_PATH);
		} else if (conf.getLong(Driver.KEY_BOUNDS_IN_DISTCACHE, -1) > 0) {
			this.bounds = DistCache.readKnownBounds(conf);
		}
		
		this.greatestItemID = conf.getInt(Driver.KEY_REBASING_MAX_ID, 1);
		this.grouper = Grouper.factory(conf);
		
		this.nbThreads = conf.getInt(Driver.KEY_NB_THREADS, 1);
	}
	
	
	protected void reduce(IntWritable gid, 
			java.lang.Iterable<TransactionWritable> transactions, Context context)
			throws java.io.IOException, InterruptedException {
		
		final Dataset dataset = TransactionWritable.buildDataset(context.getConfiguration(), transactions.iterator());
		
		context.progress(); // ping master, otherwise long mining tasks get killed
		
		if (this.phase == 1) {
			this.collector = new PerItemTopKHadoopCollector(topK, context, dataset, true, false);
		} else {
			this.collector = new PerItemTopKHadoopCollector(topK, context, dataset, false, true);
			this.collector.setKnownBounds(bounds);
		}
		
		final TIntArrayList starters = new TIntArrayList();
		this.grouper.fillWithGroupItems(starters, gid.get(), this.greatestItemID);
		
		this.collector.setGroup(new TIntHashSet(starters));
		
		final PLCM lcm = new PLCM(this.collector, this.nbThreads);
		
		FakeExtensionsIterator fake = new FakeExtensionsIterator(
				dataset.getCandidatesIterator().getSortedFrequents(), 
				starters.iterator()
			);
		
		lcm.lcm(dataset, fake);
		
		if (this.phase == 1) {
			int nbBounds = this.dumpBounds();
			context.getCounter(COUNTER_GROUP, COUNTER_BOUNDS_COUNT).increment(nbBounds);
		}
		
		this.collector.close();
	}
	
	/**
	 * @return the number of positive discovered bounds (actually written to disk) - may be 0 ! 
	 */
	protected int dumpBounds() throws IOException, InterruptedException {
		final IntWritable itemW  = new IntWritable();
		final IntWritable boundW = new IntWritable();
		
		final TIntIntMap bounds = this.collector.getTopKBounds();
		final TIntIntIterator boundsIter = bounds.iterator();
		
		int found = 0;
		
		while (boundsIter.hasNext()) {
			boundsIter.advance();
			
			int boundary = boundsIter.value();
			
			if (boundary > 0) {
				itemW.set(boundsIter.key());
				boundW.set(boundary);
				this.sideOutputs.write(BOUNDS_OUTPUT_NAME, itemW, boundW, this.boundsPath);
				found++;
			}
		}
		
		return found;
	}
	
	protected void cleanup(Context context) throws java.io.IOException, InterruptedException {
		if (this.phase == 1) {
			this.sideOutputs.close();
		}
	}
}
