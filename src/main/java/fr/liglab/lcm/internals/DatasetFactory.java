package fr.liglab.lcm.internals;

import java.util.Iterator;

import fr.liglab.lcm.io.FileReader;
import fr.liglab.lcm.mapred.writables.TransactionWritable;

public final class DatasetFactory {
	
	public static Dataset fromFile(int minsup, FileReader reader) {
		// TODO 
		// wrap reader in a copier-n-translator-to-TransactionsReader
		// add TransactionsRebasingDecorator(int[] original)
	}
	
	public static Dataset fromHadoop(int minsup, Iterator<TransactionWritable> input) {
		// TODO
		// wrap input in another copier-n-translator
		// add TransactionsFilteringDecorator(int[] original)
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
