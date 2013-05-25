package fr.liglab.lcm.internals;

import fr.liglab.lcm.internals.tidlist.TidList.TIntIterable;
import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

/**
 * This class references directly parent's tidList and transactions, but will
 * always intersect occurrences lists with its own tids. In other words, it
 * represents a projected dataset without item filtering and occurrence
 * delivery.
 */
class DatasetView extends Dataset {

	/**
	 * items known to have a 100% support in this dataset
	 */
	protected final int[] ignoreItems;

	protected final TIntIterable tids;

	/**
	 * This constructor will re-use a transactions collection and ignore some
	 * items (ignoredItem and counts' closure)
	 * 
	 * @param parent
	 *            the object containing actual transactions
	 * @param counts
	 *            item counters over viewed transactions
	 * @param viewed
	 *            viewed transactions
	 * @param ignoreItem
	 *            typically the item on which we're projecting
	 */
	DatasetView(final Dataset parent, final Counters counts, TransactionsIterable viewed, int ignoredItem) {
		super(parent.transactions, parent.tidLists);

		this.tids = viewed.tids;
		this.ignoreItems = ItemsetsFactory.extend(counts.closure, ignoredItem, parent.getIgnoredItems());
	}

	/**
	 * @return items known to have a 100% support in this dataset
	 */
	@Override
	int[] getIgnoredItems() {
		return this.ignoreItems;
	}

	@Override
	public TransactionsIterable getSupport(int item) {
		return new TransactionsIterable(new TidlistIterable(buildExtensionTIDs(item)));
	}

	/**
	 * assumes all theses TID-lists contain indexes in increasing order
	 * 
	 * @return current Tids intersected with extension's occurrences
	 */
	private TIntList buildExtensionTIDs(int extension) {
		TIntArrayList extensionTids = new TIntArrayList();

		TIntIterator myTidsIt = this.tids.iterator();
		TIntIterator parentExtTidsIt = this.tidLists.get(extension);

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

	private class TidlistIterable implements TIntIterable {
		private final TIntList l;

		public TidlistIterable(TIntList l) {
			super();
			this.l = l;
		}

		@Override
		public TIntIterator iterator() {
			return l.iterator();
		}

	}
}
