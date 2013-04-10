package fr.liglab.lcm.internals;

import java.util.Iterator;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import gnu.trove.map.TIntIntMap;

/**
 * a ConcatenatedDataset rebased at first-loading time use it with a
 * RebaserCollector
 */
public class RebasedConcatenatedCompressedDataset extends ConcatenatedCompressedDataset implements RebasedDataset {

	private Rebaser rebaser;

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
	public RebasedConcatenatedCompressedDataset(final int minimumsupport, final Iterator<int[]> transactions)
			throws DontExploreThisBranchException {

		super(minimumsupport, transactions);
	}

	@Override
	protected void prepareOccurences() {
		this.rebaser = new Rebaser(this);
		super.prepareOccurences();
	}

	@Override
	protected void filter(Iterable<int[]> transactions) {
		TIntIntMap rebasing = this.rebaser.getRebasingMap();
		TransactionsWriter tw = this.getTransactionsWriter(false);
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
				TransactionReader read = this.readTransaction(tid);
				while (read.hasNext()) {
					this.occurrences.get(read.next()).add(tid);
				}
			}
		}
	}
}
