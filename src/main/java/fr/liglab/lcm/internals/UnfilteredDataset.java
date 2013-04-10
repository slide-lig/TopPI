package fr.liglab.lcm.internals;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.Arrays;

public abstract class UnfilteredDataset extends IterableDataset {
	public static double FILTERING_THRESHOLD = 0.15;

	protected final IterableDataset parent;
	protected final TIntList tids; // indexes in parent_concatenated
	protected final int[] ignoreItems; // items known to have a 100% support

	/**
	 * builds an unfiltered projection of parentDataset over "extension"
	 * 
	 * @throws DontExploreThisBranchException
	 */
	public UnfilteredDataset(IterableDataset parentDataset, int extension) throws DontExploreThisBranchException {
		super(parentDataset.minsup, extension);

		this.parent = parentDataset;
		this.tids = parentDataset.getTidList(extension);

		this.genSupportCounts();
		this.supportCounts.remove(extension);
		this.genClosureAndFilterCount();

		this.ignoreItems = ItemsetsFactory.extend(this.discoveredClosure, extension);

		this.frequentItems = this.supportCounts.keys();
		Arrays.sort(this.frequentItems);
	}

	public UnfilteredDataset(UnfilteredDataset upper, int extension) throws DontExploreThisBranchException {

		super(upper.minsup, extension);

		this.parent = upper.parent;
		this.tids = upper.getTidList(extension);

		this.genSupportCounts();
		this.supportCounts.remove(extension);

		for (int item : upper.ignoreItems) {
			this.supportCounts.remove(item);
		}
		this.genClosureAndFilterCount();

		this.ignoreItems = ItemsetsFactory.extend(discoveredClosure, extension, upper.ignoreItems);

		this.frequentItems = this.supportCounts.keys();
		Arrays.sort(this.frequentItems);
	}

	/**
	 * assumes all theses TID-lists contain indexes in increasing order
	 * 
	 * @return current Tids intersected with extension's occurrences
	 */
	@Override
	protected TIntList getTidList(int item) {
		TIntArrayList extensionTids = new TIntArrayList();

		TIntIterator myTidsIt = this.tids.iterator();
		TIntIterator parentExtTidsIt = this.parent.getTidList(item).iterator();
		int myTid = myTidsIt.next();
		int parentExtTid = parentExtTidsIt.next();

		while (true) {

			while (myTid < parentExtTid) {
				if (!myTidsIt.hasNext()) {
					return extensionTids;
				}
				myTid = myTidsIt.next();
			}

			while (parentExtTid < myTid) {
				if (!parentExtTidsIt.hasNext()) {
					return extensionTids;
				}
				parentExtTid = parentExtTidsIt.next();
			}

			if (parentExtTid == myTid) {
				extensionTids.add(myTid);

				if (myTidsIt.hasNext()) {
					myTid = myTidsIt.next();
				} else {
					return extensionTids;
				}
				if (parentExtTidsIt.hasNext()) {
					parentExtTid = parentExtTidsIt.next();
				} else {
					return extensionTids;
				}
			}
		}
	}

	/**
	 * requires : this.parent and this.tids
	 */
	private void genSupportCounts() {
		this.supportCounts = new TIntIntHashMap();
		TIntIterator iterator = this.tids.iterator();
		while (iterator.hasNext()) {
			int tid = iterator.next();
			TransactionReader transIterator = this.parent.readTransaction(tid);
			int sup = transIterator.getTransactionSupport();
			if (sup > 0) {
				while (transIterator.hasNext()) {
					this.supportCounts.adjustOrPutValue(transIterator.next(), sup, sup);
				}
			}
		}
	}

	@Override
	public int getTransactionsCount() {
		return this.tids.size();
	}

	@Override
	protected TransactionReader readTransaction(int tid) {
		return this.parent.readTransaction(tid);
	}

	@Override
	public Dataset getProjection(int extension) throws DontExploreThisBranchException {

		double extensionSupport = this.supportCounts.get(extension);

		if ((extensionSupport / this.parent.getTransactionsCount()) > FILTERING_THRESHOLD) {
			return this.createUnfilteredDataset(this, extension);
		} else {
			return this.createFilteredDataset(this, extension, this.ignoreItems);
		}
	}

	public abstract Dataset createUnfilteredDataset(UnfilteredDataset upper, int extension)
			throws DontExploreThisBranchException;

	public abstract Dataset createFilteredDataset(IterableDataset upper, int extension, int[] ignoredItems)
			throws DontExploreThisBranchException;

	@Override
	public boolean itemsSorted() {
		return this.parent.itemsSorted();
	}

}
