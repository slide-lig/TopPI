package fr.liglab.lcm.internals;

import java.util.Iterator;

import fr.liglab.lcm.io.FileReader;
import fr.liglab.lcm.mapred.writables.TransactionWritable;
import fr.liglab.lcm.util.ItemsetsFactory;
import fr.liglab.lcm.util.ReIterableTransactionWritable;

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
		ReIterableTransactionWritable adapter = new ReIterableTransactionWritable(input);
		
		DatasetCounters counters = new DatasetCounters(minsup, adapter);
		
		Iterator<TransactionReader> reInput = adapter.reIterate();
		TransactionsFilteringDecorator filtered = new TransactionsFilteringDecorator(reInput, counters.getFrequents());
		
		return new ConcatenatedDataset(counters, filtered);
	}
	
	public static Dataset project(Dataset parent, int extension)
		throws DontExploreThisBranchException {
		
		final int firstParent = parent.ppTest(extension);
		if (firstParent >= 0) {
			throw new DontExploreThisBranchException(extension, firstParent);
		}
		
		////////////// counting //////////////////////////
		
		final int[] ignored = ItemsetsFactory.extend(parent.getItemsIgnoredForCounting(), extension);
		final int minsup = parent.counters.minSup;
		Iterator<TransactionReader> support = parent.getSupport(extension);
		
		DatasetCounters counters = new DatasetCounters(minsup, support, ignored);
		
		final int biggestClosureItem = maxItem(counters.closure);
		if (biggestClosureItem > extension) {
			throw new DontExploreThisBranchException(extension, biggestClosureItem);
		}
		
		////////////// actual projection is delegated to implementations
		
		return parent.project(extension, counters);
	}
	
	private static int maxItem(int[] of) {
		int max = Integer.MIN_VALUE;
		for(int item: of) {
			if (item > max) {
				max = item;
			}
		}
		return max;
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
