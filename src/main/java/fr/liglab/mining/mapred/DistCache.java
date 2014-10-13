package fr.liglab.mining.mapred;

import gnu.trove.impl.Constants;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Job;

/**
 * Tools for distributed cache I/O
 */
final class DistCache {
	static final String REBASINGMAP_TOKEN = "part";
	static final String PER_ITEM_BOUNDS_TOKEN = "bounds";

	/**
	 * Adds given path to conf's distributed cache
	 */
	static void copyToCache(Job job, String path) throws IOException {
		FileSystem fs = FileSystem.get(job.getConfiguration());
		Path qualifiedPath = fs.makeQualified(new Path(path));
		FileStatus[] statuses = fs.listStatus(qualifiedPath);
		// Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).warning("Filling cache with path "
		// + path);
		for (Path file : FileUtil.stat2Paths(statuses)) {
			String stringified = file.toString();
			int lastSlash = stringified.lastIndexOf('/');

			if (stringified.charAt(lastSlash + 1) != '_') {
				job.addCacheFile(file.toUri());
				// Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).warning("Filling cache with "
				// + file.toUri());
			}
		}
		// Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).warning("cache status " +
		// Arrays.toString(job.getCacheFiles()));
		fs.close();
	}

	static TIntIntMap readRebasing(URI[] files, Configuration conf) throws IOException {
		return readIntIntMap(files, conf, REBASINGMAP_TOKEN, conf.getInt(TopPIoverHadoop.KEY_REBASING_MAX_ID, 10));
	}

	static TIntIntMap readPerItemBounds(URI[] files, Configuration conf) throws IOException {
		return readIntIntMap(files, conf, PER_ITEM_BOUNDS_TOKEN, 1000);
	}

	private static TIntIntMap readIntIntMap(URI[] files, Configuration conf, String token, int size) throws IOException {
		TIntIntMap map = new TIntIntHashMap(size, Constants.DEFAULT_LOAD_FACTOR, -1, -1);
		for (URI file : files) {
			if (file.getPath().contains(token)) {
				SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(new Path(file)));
				IntWritable key = new IntWritable();
				IntWritable value = new IntWritable();

				while (reader.next(key, value)) {
					map.put(key.get(), value.get());
				}

				reader.close();
			}
		}

		return map;
	}

	public static URI[] getCachedFiles(org.apache.hadoop.mapreduce.Mapper<?, ?, ?, ?>.Context context) {
		try {
			Path[] pArray = context.getLocalCacheFiles();
			URI[] files = new URI[pArray.length];
			for (int i = 0; i < files.length; i++) {
				files[i] = pArray[i].toUri();
				if (!files[i].toASCIIString().startsWith("file")){
					files[i] = new URI("file://" + files[i].toString());
				}
			}
			return files;
		} catch (IOException e) {
			return null;
		} catch (URISyntaxException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static URI[] getCachedFiles(org.apache.hadoop.mapreduce.Reducer<?, ?, ?, ?>.Context context) {
		try {
			Path[] pArray = context.getLocalCacheFiles();
			URI[] files = new URI[pArray.length];
			for (int i = 0; i < files.length; i++) {
				files[i] = pArray[i].toUri();
				if (!files[i].toASCIIString().startsWith("file")){
					files[i] = new URI("file://" + files[i].toString());
				}
			}
			return files;
		} catch (IOException e) {
			return null;
		} catch (URISyntaxException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * @return a map which associates a rebased item ID to its original ID
	 */
	static int[] readReverseRebasing(URI[] files, Configuration conf) throws IOException {
		// Logger.getLogger(Logger.GLOBAL_LOGGER_NAME).warning("Reading reverse rebasing "
		// + files);
		final int maxId = conf.getInt(TopPIoverHadoop.KEY_REBASING_MAX_ID, -1);

		if (maxId < 0) {
			throw new IllegalArgumentException("Given configuration should contain a value for "
					+ TopPIoverHadoop.KEY_REBASING_MAX_ID);
		}

		int[] map = new int[maxId + 1];

		for (URI file : files) {
			if (file.toString().contains(REBASINGMAP_TOKEN)) {
				SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(new Path(file)));
				IntWritable key = new IntWritable();
				IntWritable value = new IntWritable();

				while (reader.next(key, value)) {
					map[value.get()] = key.get();
				}

				reader.close();
			}
		}

		return map;
	}
}