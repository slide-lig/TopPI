package fr.liglab.lcm.internals;

import java.util.Iterator;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;

/**
 * a ConcatenatedDataset rebased at first-loading time use it with a
 * RebaserCollector
 */
public class RebasedVIntConcatenatedDataset extends VIntConcatenatedDataset
		implements RebasedDataset {

	private Rebaser rebaser;
	private int totalSize = 0;

	public int[] getReverseMap() {
		return this.rebaser.getReverseMap();
	}

	/**
	 * Initial dataset constructor
	 * 
	 * the difference with parent class is in the overloaded sub-function
	 * "filter"
	 * 
	 * @throws DontExploreThisBranchException
	 */
	public RebasedVIntConcatenatedDataset(final int minimumsupport,
			final Iterator<int[]> transactions)
			throws DontExploreThisBranchException {
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
			this.totalSize += getVIntSize(rebasedItem) * counts.value();
		}
	}

	@Override
	protected void prepareTransactionsStructure(int sumOfRemainingItemsSupport,
			int distinctTransactionsLength, int distinctTransactionsCount) {
		this.concatenated = new byte[this.totalSize + nbBytesForTransSize
				* this.transactionsCount];
	}

	@Override
	protected void filter(Iterable<int[]> transactions) {
		TIntIntMap rebasing = this.rebaser.getRebasingMap();
		TransactionsWriter tw = this.getTransactionsWriter();
		for (int[] transaction : transactions) {
			boolean transactionExists = false;
			for (int item : transaction) {
				if (rebasing.containsKey(item)) {
					transactionExists = true;
					int rebased = rebasing.get(item);
					tw.addItem(rebased);
				}
			}
			if (transactionExists) {
				int tid = tw.endTransaction(1);
				TIntIterator read = this.readTransaction(tid);
				while (read.hasNext()) {
					this.occurrences.get(read.next()).add(tid);
				}
			}
		}
	}
}
