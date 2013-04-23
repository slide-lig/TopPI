package fr.liglab.lcm;

import java.util.Arrays;

import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.DatasetCounters;
import fr.liglab.lcm.internals.DatasetCounters.FrequentsIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

final class StackedJob {
	public final FrequentsIterator iterator;
	public final Dataset dataset;
	public final int[] pattern;
	public final int[] sortedfreqs;
	public final TIntIntMap failedpptests;
	private int previousItem;
	private int previousResult;

	public StackedJob(Dataset dataset, int[] pattern, FrequentsIterator it) {
		super();
		DatasetCounters counters = dataset.getCounters();
		this.iterator = it;
		this.dataset = dataset;
		this.pattern = pattern;
		this.sortedfreqs = counters.sortedFrequents;
		this.previousItem = -1;
		this.previousResult = -1;
		this.failedpptests = new TIntIntHashMap();
	}

	public synchronized void updateExploreResults(int previousItem, int previousResult) {
		if (previousItem > this.previousItem) {
			this.previousItem = previousItem;
			this.previousResult = previousResult;
		}
	}

	public void updatepptestfail(int item, int parent) {
		synchronized (this.failedpptests) {
			this.failedpptests.put(item, parent);
		}
	}

	@Override
	public String toString() {
		return "StackedJob [pattern=" + Arrays.toString(pattern) + "]";
	}

	public final int getPreviousItem() {
		return previousItem;
	}

	public final int getPreviousResult() {
		return previousResult;
	}

}