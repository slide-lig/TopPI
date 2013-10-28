package fr.liglab.mining.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.io.FileFilteredReader;
import fr.liglab.mining.mapred.Grouper.SingleGroup;
import fr.liglab.mining.mapred.writables.SupportAndTransactionWritable;
import gnu.trove.map.TIntIntMap;

public class AlternativeMiningReducer extends Reducer<IntWritable, IntWritable, IntWritable, SupportAndTransactionWritable> {
	
	private TIntIntMap rebasing;
	private int[] reverseRebasing;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		rebasing = DistCache.readRebasing(conf);
		reverseRebasing = DistCache.readReverseRebasing(conf);
	}
	
	@Override
	protected void reduce(IntWritable gidW, Iterable<IntWritable> itemsW, Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		if (conf.get(TopLCMoverHadoop.KEY_SUBDBS_BUILDER, "").toLowerCase().equals("distcache")) {
			reduceOverDistCache(gidW, itemsW, context);
		} else if (conf.get(TopLCMoverHadoop.KEY_SUBDBS_BUILDER, "").toLowerCase().equals("hdfs")) {
			reduceOverHDFS(gidW, itemsW, context);
		}
	}

	private void reduceOverHDFS(IntWritable gidW, Iterable<IntWritable> itemsW, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int gid = gidW.get();
		int minsup = conf.getInt(TopLCMoverHadoop.KEY_MINSUP, 10);
		int nbGroups = conf.getInt(TopLCMoverHadoop.KEY_NBGROUPS, 1);
		int maxItemId = conf.getInt(TopLCMoverHadoop.KEY_REBASING_MAX_ID, 1);
		
		ExplorationStep initState = null;
		
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inputStream = fs.open(new Path(conf.get(TopLCMoverHadoop.KEY_INPUT)));
		
		FileFilteredReader reader = new FileFilteredReader(inputStream, rebasing, new SingleGroup(nbGroups, maxItemId, gid));
		
		initState = new ExplorationStep(minsup, reader, maxItemId, reverseRebasing);
		LCMWrapper.mining(gid, initState, context, null, reverseRebasing);
	}

	private void reduceOverDistCache(IntWritable gidW, Iterable<IntWritable> itemsW, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		int gid = gidW.get();
		int minsup = conf.getInt(TopLCMoverHadoop.KEY_MINSUP, 10);
		int nbGroups = conf.getInt(TopLCMoverHadoop.KEY_NBGROUPS, 1);
		int maxItemId = conf.getInt(TopLCMoverHadoop.KEY_REBASING_MAX_ID, 1);
		
		ExplorationStep initState = null;
		
		for (Path path : DistributedCache.getLocalCacheFiles(conf)) {
			if (path.toString().contains(conf.get(TopLCMoverHadoop.KEY_INPUT))) {
				SingleGroup filter = new SingleGroup(nbGroups, maxItemId, gid);
				FileFilteredReader reader = new FileFilteredReader(path.toString(), rebasing, filter);
				
				initState = new ExplorationStep(minsup, reader, maxItemId, reverseRebasing);
			}
		}
		
		LCMWrapper.mining(gid, initState, context, null, reverseRebasing);
	}
}
