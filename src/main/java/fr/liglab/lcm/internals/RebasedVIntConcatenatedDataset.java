package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.Iterator;

import org.omg.CORBA.IntHolder;

/**
 * a ConcatenatedDataset rebased at first-loading time use it with a
 * RebaserCollector
 */
public class RebasedVIntConcatenatedDataset extends VIntConcatenatedDataset
		implements RebasedDataset {

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
	public RebasedVIntConcatenatedDataset(final int minimumsupport,
			final Iterator<int[]> transactions) {
		super(minimumsupport, transactions);
	}

	@Override
	protected int prepareOccurences() {

		// Rebaser instanciation will nullify supportCounts - grab it while it's
		// there !
		TIntIntIterator counts = this.supportCounts.iterator();
		int totalSize = 0;
		this.rebaser = new Rebaser(this);
		TIntIntMap rebasing = this.rebaser.getRebasingMap();

		while (counts.hasNext()) {
			counts.advance();
			int rebasedItem = rebasing.get(counts.key());
			this.occurrences
					.put(rebasedItem, new TIntArrayList(counts.value()));
			totalSize += getVIntSize(rebasedItem) * counts.value();
		}
		return totalSize;
	}

	@Override
	protected void filter(Iterable<int[]> transactions) {
		TIntIntMap rebasing = this.rebaser.getRebasingMap();
		IntHolder concIndex = new IntHolder(0);
		for (int[] transaction : transactions) {
			int startAt = concIndex.value;
			for (int item : transaction) {
				if (rebasing.containsKey(item)) {
					int rebased = rebasing.get(item);
					writeVInt(this.concatenated, concIndex, rebased);
				}
			}
			if (concIndex.value != startAt) {
				int newTransId = concIndex.value;
				writeVInt(this.concatenated, concIndex, concIndex.value
						- startAt);
				IntHolder addIndex = new IntHolder(startAt);
				// new format sets transaction length after the data, so we only
				// know the transId at the end, we need a new loop
				// also size is in bytes and not number of items
				while (addIndex.value < newTransId) {
					int rebased = readVInt(this.concatenated, addIndex);
					this.occurrences.get(rebased).add(newTransId);
				}
			}
		}
	}
}
