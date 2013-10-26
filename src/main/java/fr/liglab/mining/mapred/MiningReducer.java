package fr.liglab.mining.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.mapred.writables.ConcatenatedTransactionsWritable;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;

public class MiningReducer extends
		Reducer<IntWritable, ConcatenatedTransactionsWritable, IntWritable, SupportAndTransactionWritable> {
	
	private int[] reverseRebasing = null;
	private MultipleOutputs<IntWritable, SupportAndTransactionWritable> sideOutputs = null;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		if (this.reverseRebasing != null) {
			return;
		}
		
		Configuration conf = context.getConfiguration();
		this.reverseRebasing = DistCache.readReverseRebasing(conf);
		
		if (conf.getInt(TopLCMoverHadoop.KEY_METHOD, 0) == 2) {
			if (conf.get(LCMWrapper.KEY_BOUNDS_PATH) != null) {
				this.sideOutputs = new MultipleOutputs<IntWritable, SupportAndTransactionWritable>(context);
			}
		}
	}
	
	@Override
	protected void reduce(IntWritable gidW, 
			Iterable<ConcatenatedTransactionsWritable> transactions, Context context)
			throws IOException, InterruptedException {
		
		final int gid = gidW.get();
		final Configuration conf = context.getConfiguration();
		
		final int minsup   = conf.getInt(TopLCMoverHadoop.KEY_MINSUP, 1000);
		final int maxId    = conf.getInt(TopLCMoverHadoop.KEY_REBASING_MAX_ID, 1);

		// yeah it's ugly, but SubDatasetConverter holds a copy of the dataset so we'd 
		// rather suggest to the VM this is a short-lasting object
		ExplorationStep initState = new ExplorationStep(minsup, 
				new SubDatasetConverter(transactions.iterator())
				, maxId, this.reverseRebasing);
		
		LCMWrapper.mining(gid, initState, context, this.sideOutputs, this.reverseRebasing);
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		if (this.sideOutputs != null) {
			this.sideOutputs.close();
		}
	}
}





