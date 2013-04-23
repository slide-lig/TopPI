package fr.liglab.lcm.internals;

import java.util.Iterator;

import fr.liglab.lcm.io.FileReader;
import fr.liglab.lcm.mapred.writables.TransactionWritable;

public final class DatasetFactory {
	
	/**
	 * @param minsup
	 * @param path on local filesystem to an ASCII transaction database (@see fr.liglab.lcm.io.FileReader)
	 * @return a rebased dataset
	 */
	public static Dataset fromFile(int minsup, String path) {
		FileReader reader = new FileReader(path);
		
		DatasetRebaserCounters counters = new DatasetRebaserCounters(minsup, reader);
		reader.close();
		
		reader = new FileReader(path);
		TransactionsRebasingDecorator filtered = new TransactionsRebasingDecorator(reader, counters.rebaseMap);
		
		return new ConcatenatedDataset(counters, filtered);
	}
	
	public static Dataset fromHadoop(int minsup, Iterator<TransactionWritable> input) {
		// TODO
		// wrap input in another copier-n-translator
		// add TransactionsSortingDecorator(int[] original)
	}
	
	public static Dataset project(Dataset parent, int extension)
		throws DontExploreThisBranchException {
		
		
		/**
		 * if (parent instanceof concatenatedDatasetView)
		 * 	parent.getIgnoredItems
		 * 
		 * if (parent.ppTest >= 0)
		 * 	throw new DontExploreThisBranchException
		 * 
		 * counter = new DatasetCounters(parent.getCounters().minSup, parent.getSupport(extension), ignoredItems + extension )
		 * 
		 * if (max(counter.closure) > extension))
		 * 	throw new DontExploreThisBranchException
		 * 
		 * 
		 * then invoke actual dataset's constructor
		 * 
		 */
	}
	
	/**
	 * Thrown when you shouldn't try to work on an extension
	 */
	public static class DontExploreThisBranchException extends Exception {
	        private static final long serialVersionUID = 2969583589161047791L;
	
	        public final int firstParent;
	        public final int extension;
	
	        /**
	         * @param extension the tested extension
	         * @param foundFirstParent
	         *            a item found in closure > extension
	         */
	        public DontExploreThisBranchException(int exploredExtension, int foundFirstParent) {
	                this.firstParent = foundFirstParent;
	                this.extension = exploredExtension;
	        }
	}
}
