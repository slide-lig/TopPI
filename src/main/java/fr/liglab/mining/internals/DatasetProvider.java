package fr.liglab.mining.internals;

import java.util.Map.Entry;
import java.util.TreeMap;

public class DatasetProvider {
	private final double dampingFactor = 0.95;
	private final TreeMap<Integer, Dataset> datasets;

	public DatasetProvider(Dataset dataset, int minimumSupport, int transactionsCount) {
		this.datasets = new TreeMap<Integer, Dataset>();
		this.datasets.put(Integer.valueOf(minimumSupport), dataset);
		this.datasets.put(Integer.valueOf(1000), new Dataset(dataset.transactions, dataset.tidLists, 1000, 10000));
		this.datasets.put(Integer.valueOf(10000), new Dataset(dataset.transactions, dataset.tidLists, 10000, 1000));
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
