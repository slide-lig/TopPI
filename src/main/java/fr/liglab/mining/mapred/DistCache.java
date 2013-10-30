package fr.liglab.mining.mapred;

import gnu.trove.impl.Constants;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;

/**
 * Tools for distributed cache I/O
 */
final class DistCache {
	static final String REBASINGMAP_DIRNAME = "rebasing";
	static final String PER_ITEM_BOUNDS_DIRNAME = "perItemBounds";
	
	/**
	 * Adds given path to conf's distributed cache
	 */
	static void copyToCache(Configuration conf, String path) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path qualifiedPath = fs.makeQualified(new Path(path));
		FileStatus[] statuses = fs.listStatus(qualifiedPath);
		
		for (Path file : FileUtil.stat2Paths(statuses)) {
			String stringified = file.toString();
			int lastSlash = stringified.lastIndexOf('/');
			
			if (stringified.charAt(lastSlash+1) != '_') {
				DistributedCache.addCacheFile(file.toUri(), conf);
				System.out.println(file.toUri().toString());
			}
		}
	}
	
	static TIntIntMap readRebasing(Configuration conf) throws IOException {
		return readIntIntMap(conf, REBASINGMAP_DIRNAME, conf.getInt(TopLCMoverHadoop.KEY_REBASING_MAX_ID, 10));
	}
	
	static TIntIntMap readPerItemBounds(Configuration conf) throws IOException {
		return readIntIntMap(conf, PER_ITEM_BOUNDS_DIRNAME, 1000);
	}
	
	private static TIntIntMap readIntIntMap(Configuration conf, String token, int size) throws IOException {
		TIntIntMap map = new TIntIntHashMap(size, Constants.DEFAULT_LOAD_FACTOR, -1, -1);
		FileSystem fs = FileSystem.getLocal(conf);
		
		for (Path path : DistributedCache.getLocalCacheFiles(conf)) {
			if (path.toString().contains(token)) {
				Reader reader = new Reader(fs, path, conf);
				IntWritable key = new IntWritable();
				IntWritable value = new IntWritable();
				
				while(reader.next(key, value)) {
					map.put(key.get(), value.get());
				}
				
				reader.close();
			}
		}
		
		return map;
	}
	
	/**
	 * @return a map which associates a rebased item ID to its original ID
	 */
	static int[] readReverseRebasing(Configuration conf) throws IOException {
		final int maxId = conf.getInt(TopLCMoverHadoop.KEY_REBASING_MAX_ID, -1);
		
		if (maxId < 0) {
			throw new IllegalArgumentException("Given configuration should contain a value for "+TopLCMoverHadoop.KEY_REBASING_MAX_ID);
		}
		
		int[] map = new int[maxId+1];
		
		FileSystem fs = FileSystem.getLocal(conf);
		
		for (Path path : DistributedCache.getLocalCacheFiles(conf)) {
			if (path.toString().contains(REBASINGMAP_DIRNAME)) {
				Reader reader = new Reader(fs, path, conf);
				IntWritable key = new IntWritable();
				IntWritable value = new IntWritable();
				
				while(reader.next(key, value)) {
					map[value.get()] = key.get();
				}
				
				reader.close();
			}
		}
		
		return map;
	}
}