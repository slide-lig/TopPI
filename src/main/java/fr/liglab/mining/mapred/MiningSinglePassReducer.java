package fr.liglab.mining.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.mining.TopLCM;
import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.internals.FrequentsIterator;
import fr.liglab.mining.internals.FrequentsIteratorRenamer;
import fr.liglab.mining.io.PatternsCollector;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.mapred.writables.ConcatenatedTransactionsWritable;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;

public class MiningSinglePassReducer extends
		Reducer<IntWritable, ConcatenatedTransactionsWritable, NullWritable, SupportAndTransactionWritable> {
	
	private int[] reverseRebasing = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		if (this.reverseRebasing == null) {
			Configuration conf = context.getConfiguration();
			this.reverseRebasing = DistCache.readReverseRebasing(conf);
		}
	}
	
	@Override
	protected void reduce(IntWritable gidW, 
			Iterable<ConcatenatedTransactionsWritable> transactions, Context context)
			throws IOException, InterruptedException {
		
		final int gid = gidW.get();
		final Configuration conf = context.getConfiguration();
		
		final int k        = conf.getInt(TopLCMoverHadoop.KEY_K, 1);
		final int minsup   = conf.getInt(TopLCMoverHadoop.KEY_MINSUP, 1000);
		final int maxId    = conf.getInt(TopLCMoverHadoop.KEY_REBASING_MAX_ID, 1);
		final int nbGroups = conf.getInt(TopLCMoverHadoop.KEY_NBGROUPS, 1);
		
		ExplorationStep initState = new ExplorationStep(minsup, 
				new SubDatasetConverter(transactions.iterator())
				, maxId, this.reverseRebasing);
		// yeah it's ugly, but SubDatasetConverter holds a copy of the dataset so we'd 
		// rather suggest to the VM this is a short-lasting object
		context.progress();
		
		Grouper grouper = new Grouper(nbGroups, maxId);
		
		PatternsCollector collector = new HadoopPatternsCollector(context);
		
		FrequentsIterator groupItems = grouper.getGroupItems(gid);
		groupItems = new FrequentsIteratorRenamer(groupItems, this.reverseRebasing);
		collector = new PerItemTopKCollector(collector, k, maxId, groupItems);
		
		TopLCM miner = new TopLCM(collector, 1);
		// TODO invoke context.progress sometimes
		miner.lcm(initState);
		
		context.progress();
		collector.close();
	}
}
