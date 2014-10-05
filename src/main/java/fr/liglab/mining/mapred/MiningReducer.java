package fr.liglab.mining.mapred;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.ws.Holder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.io.FileFilteredReader;
import fr.liglab.mining.mapred.Grouper.SingleGroup;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;
import fr.liglab.mining.util.ProgressWatcherThread;

public class MiningReducer extends Reducer<IntWritable, IntWritable, IntWritable, SupportAndTransactionWritable> {
	
	private int[] reverseRebasing;
	private MultipleOutputs<IntWritable, SupportAndTransactionWritable> sideOutputs = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		this.reverseRebasing = DistCache.readReverseRebasing(conf);
		
		if (conf.get(MinerWrapper.KEY_BOUNDS_PATH) != null) {
			this.sideOutputs  = new MultipleOutputs<IntWritable, SupportAndTransactionWritable>(context);
		}
	}
	
	@Override
	protected void reduce(IntWritable gidW, Iterable<IntWritable> itemsW, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int gid = gidW.get();
		int minsup = conf.getInt(TopPIoverHadoop.KEY_MINSUP, 10);
		int nbGroups = conf.getInt(TopPIoverHadoop.KEY_NBGROUPS, 1);
		int maxItemId = conf.getInt(TopPIoverHadoop.KEY_REBASING_MAX_ID, 1);
		final int k = conf.getInt(TopPIoverHadoop.KEY_K, 1);

		ProgressWatcherThread coucou = new ProgressWatcherThread();
		coucou.setHadoopContext(context);
		coucou.start();
		
		boolean manyItems = conf.getBoolean(TopPIoverHadoop.KEY_MANY_ITEMS_MODE, false);
		String marker = manyItems ? TopPIoverHadoop.FILTERED_DIRNAME : conf.get(TopPIoverHadoop.KEY_INPUT);
		List<Path> cached = new ArrayList<Path>();
		
		for (Path path : DistributedCache.getLocalCacheFiles(conf)) {
			if (path.toString().contains(marker)) {
				cached.add(path);
			}
		}

		SingleGroup filter = new SingleGroup(nbGroups, maxItemId, gid);
		Holder<int[]> renaming = new Holder<int[]>();
		ExplorationStep initState = null;

		if (manyItems) {
			FilteredDatasetsReader reader = new FilteredDatasetsReader(cached, conf, filter);
			initState = new ExplorationStep(minsup, reader, maxItemId, this.reverseRebasing, renaming, k);
		} else {
			if (cached.size() > 1) {
				throw new RuntimeException("Without 'many items mode' a single input file is expected in the distcache");
			}
			Path path = cached.get(0);
			FileFilteredReader reader = new FileFilteredReader(path.toString(), DistCache.readRebasing(conf), filter);
			initState = new ExplorationStep(minsup, reader, maxItemId, this.reverseRebasing, renaming, k);
		}
		
		System.err.println("GROUP "+gid+": "+initState.counters.toString());
		
		coucou.interrupt();
		coucou = null;
		
		MinerWrapper.mining(gid, initState, context, this.sideOutputs, this.reverseRebasing, renaming);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		if (this.sideOutputs != null) {
			this.sideOutputs.close();
		}
	}
}
