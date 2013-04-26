package fr.liglab.lcm.mapred;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;

/**
 * Tooling functions for distributed cache I/O
 */
class DistCache {
	static final String REBASINGMAP_DIRNAME = "rebasing";
	static final String BOUNDS_DIRNAME = "bounds";
	
	/**
	 * @return a map which associates (to any frequent item) its new item ID
	 */
	static TIntIntMap readItemsRebasing(Configuration conf) throws IOException {
		return readIntIntMap(conf, REBASINGMAP_DIRNAME);
	}

	/**
	 * @return a map which associates (to any frequent item, if known) its k-th pattern's support count 
	 */
	static TIntIntMap readKnownBounds(Configuration conf) throws IOException {
		return readIntIntMap(conf, BOUNDS_DIRNAME);
	}
	
	private static TIntIntMap readIntIntMap(Configuration conf, String pathFilter) throws IOException {
		TIntIntMap map = new TIntIntHashMap();
		
		FileSystem fs = FileSystem.getLocal(conf);
		
		for (Path path : DistributedCache.getLocalCacheFiles(conf)) {
			if (path.toString().contains(pathFilter)) {
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
	static TIntIntMap readReverseRebasing(Configuration conf) throws IOException {
		TIntIntMap map = new TIntIntHashMap();
		
		FileSystem fs = FileSystem.getLocal(conf);
		
		for (Path path : DistributedCache.getLocalCacheFiles(conf)) {
			if (path.toString().contains(REBASINGMAP_DIRNAME)) {
				Reader reader = new Reader(fs, path, conf);
				IntWritable key = new IntWritable();
				IntWritable value = new IntWritable();
				
				while(reader.next(key, value)) {
					map.put(value.get(), key.get());
				}
				
				reader.close();
			}
		}
		
		return map;
	}
}
