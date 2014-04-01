package fr.liglab.mining.internals;

import java.util.Map.Entry;
import java.util.TreeMap;

public class DatasetProvider {
	/**
	 * If non-null, this should be an array of frequency thresholds for which you want 
	 * pre-filtered datasets 
	 */
	public static Integer[] toBePreFiltered = null;
	
	private final double dampingFactor = 0.95;
	private final TreeMap<Integer, Dataset> datasets;

	public DatasetProvider(ExplorationStep target) {
		this.datasets = new TreeMap<Integer, Dataset>();
		this.datasets.put(target.counters.minSupport, target.dataset);
		
		if (toBePreFiltered != null) {
			//System.err.println("Starting prefiltering at "+System.currentTimeMillis());
			Thread[] workers = new Thread[toBePreFiltered.length];
			for (int i = 0; i < toBePreFiltered.length; i++) {
				workers[i] = new FilteringThread(target, toBePreFiltered[i]);
				workers[i].start();
			}
			
			for (Thread thread : workers) {
				try {
					thread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			//System.err.println("Done prefiltering at "+System.currentTimeMillis());
		}
	}

	public Dataset getDatasetForSupportThreshold(int supportThreshold) {
		@SuppressWarnings("boxing")
		Entry<Integer, Dataset> floorEntry = this.datasets.floorEntry(supportThreshold);
		if (floorEntry == null) {
			return this.datasets.firstEntry().getValue();
		} else {
			return floorEntry.getValue();
		}
	}

	public Dataset getDatasetForItem(int item, int knownPreviousSupportThreshold) {
		if (item < ExplorationStep.INSERT_UNCLOSED_UP_TO_ITEM) {
			return this.getDatasetForSupportThreshold((int) Math.floor(knownPreviousSupportThreshold
					* this.dampingFactor));
		} else {
			return this.datasets.firstEntry().getValue();
		}
	}
	
	private class FilteringThread extends Thread {
		
		private ExplorationStep source;
		private Integer minSup;

		FilteringThread(ExplorationStep from, Integer minSup){
			this.source = from;
			this.minSup = minSup;
		}
		
		 @Override
		public void run() {
			DenseCounters filtered = new DenseCounters((DenseCounters) source.counters, minSup);
			
			TransactionsFilteringDecorator transactions = new TransactionsFilteringDecorator(
					source.dataset.getTransactions(), filtered.getSupportCounts(), true);
			
			Dataset dataset = new Dataset(filtered, transactions, minSup, filtered.maxFrequent);
			
			datasets.put(minSup, dataset);
		}
	}
}
