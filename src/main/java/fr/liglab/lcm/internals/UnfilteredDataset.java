package fr.liglab.lcm.internals;

import java.util.Arrays;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;

public abstract class UnfilteredDataset extends IterableDataset {
	public static final double FILTERING_THRESHOLD = 0.15;

	protected final IterableDataset parent;
	protected final TIntList tids; // indexes in parent_concatenated
	protected final int[] frequentItems;
	protected final int[] ignoreItems; // items known to have a 100% support

	/**
	 * builds an unfiltered projection of parentDataset over "extension"
	 * 
	 * @throws DontExploreThisBranchException
	 */
	public UnfilteredDataset(IterableDataset parentDataset, int extension)
			throws DontExploreThisBranchException {
		super(parentDataset.minsup, extension);

		this.parent = parentDataset;
		this.tids = parentDataset.getTidList(extension);

		this.genSupportCounts();
		this.supportCounts.remove(extension);
		this.genClosureAndFilterCount();

		this.ignoreItems = ItemsetsFactory.extend(this.discoveredClosure,
				extension);

		this.frequentItems = this.supportCounts.keys();
		Arrays.sort(this.frequentItems);
	}

	public UnfilteredDataset(UnfilteredDataset upper, int extension,
			TIntArrayList extensionTids) throws DontExploreThisBranchException {

		super(upper.minsup, extension);

		this.parent = upper.parent;
		this.tids = extensionTids;

		this.genSupportCounts();
		this.supportCounts.remove(extension);

		for (int item : upper.ignoreItems) {
			this.supportCounts.remove(item);
		}
		this.genClosureAndFilterCount();

		this.ignoreItems = ItemsetsFactory.extend(discoveredClosure, extension,
				upper.ignoreItems);

		this.frequentItems = this.supportCounts.keys();
		Arrays.sort(this.frequentItems);
	}

	@Override
	protected TIntList getTidList(int item) {
		return this.parent.getTidList(item);
	}

	/**
	 * requires : this.parent and this.tids
	 */
	private void genSupportCounts() {
		this.supportCounts = new TIntIntHashMap();

		TIntIterator iterator = this.tids.iterator();
		while (iterator.hasNext()) {
			int tid = iterator.next();
			TIntIterator transIterator = this.parent.readTransaction(tid);
			while (transIterator.hasNext()) {
				this.supportCounts.adjustOrPutValue(transIterator.next(), 1, 1);
			}
		}
	}

	@Override
	public int getTransactionsCount() {
		return this.tids.size();
	}

	@Override
	protected TIntIterator readTransaction(int tid) {
		return this.parent.readTransaction(tid);
	}

	@Override
	public Dataset getProjection(int extension)
			throws DontExploreThisBranchException {

		TIntArrayList extensionTids = buildExtensionTIDs(extension);
		double extensionSupport = this.supportCounts.get(extension);

		if ((extensionSupport / this.parent.getTransactionsCount()) > FILTERING_THRESHOLD) {
			return this.createUnfilteredDataset(this, extension, extensionTids);
		} else {
			return this.createFilteredDataset(this.parent, extension,
					extensionTids, this.ignoreItems);
		}
	}

	public abstract Dataset createUnfilteredDataset(UnfilteredDataset upper,
			int extension, TIntArrayList extensionTid)
			throws DontExploreThisBranchException;

	public abstract Dataset createFilteredDataset(IterableDataset upper,
			int extension, TIntArrayList extensionTids, int[] ignoredItems)
			throws DontExploreThisBranchException;

	/**
	 * assumes all theses TID-lists contain indexes in increasing order
	 * 
	 * @return current Tids intersected with extension's occurrences
	 */
	private TIntArrayList buildExtensionTIDs(int extension) {
		TIntArrayList extensionTids = new TIntArrayList();

		TIntIterator myTidsIt = this.tids.iterator();
		TIntIterator parentExtTidsIt = this.parent.getTidList(extension)
				.iterator();

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

	@Override
	public ExtensionsIterator getCandidatesIterator() {
		return new CandidatesIterator();
	}

	public class CandidatesIterator implements ExtensionsIterator {

		private int i = 0;
		private final int candidatesLength; // candidates is

		// frequentItems[0:candidatesLength[

		public CandidatesIterator() {
			int coreItemIndex = Arrays.binarySearch(frequentItems, coreItem);
			if (coreItemIndex >= 0) {
				throw new RuntimeException(
						"Unexpected : coreItem appears in frequentItems !");
			}

			// binarySearch returns -(insertion_point)-1
			// where insertion_point == index of first element greater OR
			// a.length
			this.candidatesLength = -coreItemIndex - 1;
		}

		public int[] getSortedFrequents() {
			return frequentItems;
		}

		public int getExtension() {
			if (i < this.candidatesLength) {
				return frequentItems[i++];
			} else {
				return -1;
			}
		}
	}
}
