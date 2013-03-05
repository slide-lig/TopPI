package fr.liglab.lcm.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * Tooling functions for distributed cache I/O
 */
class DistCache {
	static final String GROUPSMAP_DIRNAME = "itemGroups";
	
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
}
