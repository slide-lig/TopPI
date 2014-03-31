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
			for (Integer minsup : toBePreFiltered) {
				preFilter(target, minsup);
			}
		}
	}
	
	private void preFilter(ExplorationStep from, Integer minSup) {
		DenseCounters filtered = new DenseCounters((DenseCounters) from.counters, minSup);
		
		TransactionsFilteringDecorator transactions = new TransactionsFilteringDecorator(
				from.dataset.getTransactions(), filtered.getSupportCounts(), true);
		
		Dataset dataset = new Dataset(filtered, transactions, minSup, filtered.maxFrequent);
		
		this.datasets.put(minSup, dataset);
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
}
