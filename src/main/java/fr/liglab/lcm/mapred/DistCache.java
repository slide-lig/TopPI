package fr.liglab.lcm.mapred;

import fr.liglab.lcm.mapred.writables.GIDandRebaseWritable;
import fr.liglab.lcm.util.RebasingAndGroupID;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;

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
 * Tooling functions for distributed cache I/O
 */
class DistCache {
	static final String GROUPSMAP_DIRNAME = "itemGroups";
	
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
			}
		}
	}
	
	/**
	 * @return a map which associates (to any frequent item) its group ID and its new item ID
	 */
	static TIntObjectMap<RebasingAndGroupID> readItemsDispatching(Configuration conf) throws IOException {
		TIntObjectMap<RebasingAndGroupID> map = new TIntObjectHashMap<RebasingAndGroupID>();
		
		FileSystem fs = FileSystem.getLocal(conf);
		
		for (Path path : DistributedCache.getLocalCacheFiles(conf)) {
			if (path.toString().contains(GROUPSMAP_DIRNAME)) {
				Reader reader = new Reader(fs, path, conf);
				IntWritable key = new IntWritable();
				GIDandRebaseWritable v = new GIDandRebaseWritable();
				
				while(reader.next(key, v)) {
					RebasingAndGroupID entry = new RebasingAndGroupID(v.getRebased(), v.getGid());
					map.put(key.get(), entry);
				}
				
				reader.close();
			}
		}
		
		return map;
	}
	
	/**
	 * @return list of (rebased) items for given group ID  
	 */
	static TIntArrayList readStartersFor(Configuration conf, int gid) throws IOException {
		TIntArrayList starters = new TIntArrayList();
		
		FileSystem fs = FileSystem.getLocal(conf);
		
		for (Path path : DistributedCache.getLocalCacheFiles(conf)) {
			if (path.toString().contains(GROUPSMAP_DIRNAME)) {
				Reader reader = new Reader(fs, path, conf);
				IntWritable key = new IntWritable();
				GIDandRebaseWritable v = new GIDandRebaseWritable();
				
				while(reader.next(key, v)) {
					if (v.getGid() == gid) {
						starters.add(v.getRebased());
					}
				}
				
				reader.close();
			}
		}
		
		return starters;
	}
	
	/**
	 * @return a map which associates a rebased item ID to its original ID
	 */
	static TIntIntMap readReverseRebasing(Configuration conf) throws IOException {
		TIntIntMap map = new TIntIntHashMap();
		
		FileSystem fs = FileSystem.getLocal(conf);
		
		for (Path path : DistributedCache.getLocalCacheFiles(conf)) {
			if (path.toString().contains(GROUPSMAP_DIRNAME)) {
				Reader reader = new Reader(fs, path, conf);
				IntWritable key = new IntWritable();
				GIDandRebaseWritable v = new GIDandRebaseWritable();
				
				while(reader.next(key, v)) {
					map.put(v.getRebased(), key.get());
				}
				
				reader.close();
			}
		}
		
		return map;
	}
}
