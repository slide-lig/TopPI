package fr.liglab.lcm;

import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.ExtensionsIterator;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.Arrays;

public final class StackedJob {
	private final ExtensionsIterator iterator;
	private final Dataset dataset;
	private final int[] pattern;
	private final int[] sortedfreqs;
	private final TIntIntMap failedpptests;
	private int previousItem;
	private int previousResult;

	public StackedJob(ExtensionsIterator iterator, Dataset dataset, int[] pattern, int[] sortedfreqs) {
		super();
		this.iterator = iterator;
		this.dataset = dataset;
		this.pattern = pattern;
		this.sortedfreqs = sortedfreqs;
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

	public final ExtensionsIterator getIterator() {
		return iterator;
	}

	public final Dataset getDataset() {
		return dataset;
	}

	public final int[] getPattern() {
		return pattern;
	}

	public final int[] getSortedfreqs() {
		return sortedfreqs;
	}

	public final TIntIntMap getFailedpptests() {
		return failedpptests;
	}

	public final int getPreviousItem() {
		return previousItem;
	}

	public final int getPreviousResult() {
		return previousResult;
	}

}