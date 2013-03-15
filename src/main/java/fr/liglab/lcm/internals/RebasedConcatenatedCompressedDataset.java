package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;

import java.util.Arrays;
import java.util.Iterator;

/**
 * a ConcatenatedDataset rebased at first-loading time use it with a
 * RebaserCollector
 */
public class RebasedConcatenatedCompressedDataset extends
		ConcatenatedCompressedDataset implements RebasedDataset {

	private Rebaser rebaser;

	public int[] getReverseMap() {
		return this.rebaser.getReverseMap();
	}

	/**
	 * Initial dataset constructor
	 * 
	 * the difference with parent class is in the overloaded sub-function
	 * "filter"
	 */
	public RebasedConcatenatedCompressedDataset(final int minimumsupport,
			final Iterator<int[]> transactions) {
		super(minimumsupport, transactions);
	}

	@Override
	protected void prepareOccurences() {

		// Rebaser instanciation will nullify supportCounts - grab it while it's
		// there !
		TIntIntIterator counts = this.supportCounts.iterator();

		this.rebaser = new Rebaser(this);
		TIntIntMap rebasing = this.rebaser.getRebasingMap();

		while (counts.hasNext()) {
			counts.advance();
			int rebasedItem = rebasing.get(counts.key());
			this.occurrences
					.put(rebasedItem, new TIntArrayList(counts.value()));
		}
	}

	@Override
	protected TIntArrayList filter(Iterable<int[]> transactions) {
		TIntIntMap rebasing = this.rebaser.getRebasingMap();
		TIntArrayList transactionsList = new TIntArrayList();
		int i = 2;
		int tIndex = 1;

		for (int[] transaction : transactions) {
			int length = 0;

			for (int item : transaction) {
				if (rebasing.containsKey(item)) {
					int rebased = rebasing.get(item);
					this.concatenated[i] = rebased;
					this.occurrences.get(rebased).add(tIndex);
					length++;
					i++;
				}
			}

			if (length > 0) {
				transactionsList.add(tIndex);
				this.concatenated[tIndex] = length;
				this.concatenated[tIndex - 1] = 1;
				Arrays.sort(this.concatenated, tIndex + 1, i);
				i++;
				tIndex = i;
				i++;
			}
		}
		return transactionsList;
	}
}
