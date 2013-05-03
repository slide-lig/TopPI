package fr.liglab.lcm.internals.nomaps;

import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

import fr.liglab.lcm.internals.TransactionReader;
import fr.liglab.lcm.internals.tidlist.TidList.TIntIterable;
import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

/**
 * Exploits transactions concatenated in another ConcatenatedDataset, keeping
 * only those containing an item
 * 
 * It does NOT stores transactions' weights, nor it performs ppTest and
 * occurrence delivery.
 */
class FlexibleDatasetView extends FlexibleDataset {
	static final double THRESHOLD = 0.15;

	protected final int[] ignoreItems; // items known to have a 100% support in
										// parent.concatenated
	protected final FlexibleDataset parent;
	protected final TIntIterable tids;

	protected TIntIterable latestBuilt = null;
	protected int latestBuiltExtension = Integer.MIN_VALUE;

	FlexibleDatasetView(final DatasetCountersRenamer counts, FlexibleDataset upper, int extension) {
		super(counts, (upper instanceof FlexibleDatasetView) ? ((FlexibleDatasetView) upper).parent.transactions
				: upper.transactions);

		if (upper instanceof FlexibleDatasetView) {
			FlexibleDatasetView upperView = (FlexibleDatasetView) upper;
			this.parent = upperView.parent;
			this.tids = upperView.getExtensionTIDs(extension);
			this.ignoreItems = ItemsetsFactory.extend(counts.closure, extension, upperView.ignoreItems);
		} else {
			this.parent = upper;
			this.tids = this.parent.tidLists.getIterable(extension);
			this.ignoreItems = ItemsetsFactory.extend(counts.closure, extension);
		}
	}

	@Override
	final int[] getItemsIgnoredForCounting() {
		return this.ignoreItems;
	}

	@Override
	protected final double getConcatenatedTransactionCount() {
		return this.parent.counters.transactionsCount;
	}

	@Override
	public Iterator<TransactionReader> getSupport(int item) {
		final TIntIterator it = getExtensionTIDs(item).iterator();
		return new Iterator<TransactionReader>() {

			@Override
			public void remove() {
				throw new NotImplementedException();
			}

			@Override
			public TransactionReader next() {
				return transactions.get(it.next());
			}

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}
		};
	}

	/**
	 * buildExtensionTIDs with memoization : it's usually invoked twice for the
	 * same extension : once by getSupport and, likely, again by constructor
	 */
	private synchronized TIntIterable getExtensionTIDs(int extension) {
		if (this.latestBuiltExtension != extension) {
			this.latestBuilt = buildExtensionTIDs(extension);
			this.latestBuiltExtension = extension;
		}

		return this.latestBuilt;
	}

	/**
	 * assumes all theses TID-lists contain indexes in increasing order
	 * 
	 * @return current Tids intersected with extension's occurrences
	 */
	private TIntIterable buildExtensionTIDs(int extension) {
		TIntArrayList extensionTids = new TIntArrayList();

		TIntIterator myTidsIt = this.tids.iterator();
		TIntIterator parentExtTidsIt = this.parent.tidLists.get(extension);

		int myTid = myTidsIt.next();
		int parentExtTid = parentExtTidsIt.next();

		while (true) {

			while (myTid < parentExtTid) {
				if (!myTidsIt.hasNext()) {
					return new TidlistIterable(extensionTids);
				}
				myTid = myTidsIt.next();
			}

			while (parentExtTid < myTid) {
				if (!parentExtTidsIt.hasNext()) {
					return new TidlistIterable(extensionTids);
				}
				parentExtTid = parentExtTidsIt.next();
			}

			if (parentExtTid == myTid) {
				extensionTids.add(myTid);

				if (myTidsIt.hasNext()) {
					myTid = myTidsIt.next();
				} else {
					return new TidlistIterable(extensionTids);
				}
				if (parentExtTidsIt.hasNext()) {
					parentExtTid = parentExtTidsIt.next();
				} else {
					return new TidlistIterable(extensionTids);
				}
			}
		}
	}

	@Override
	Dataset project(int extension, DatasetCountersRenamer extensionCounters) {
		double reductionRate = extensionCounters.transactionsCount / this.getConcatenatedTransactionCount();
		if (this.counters.renaming != null) {
			extensionCounters.updateReverseRenaming(this.counters.renaming);
		}
		if (!this.longTransactionMode && reductionRate > FlexibleDatasetView.THRESHOLD) {
			return new FlexibleDatasetView(extensionCounters, this, extension);
		} else {
			Iterator<TransactionReader> support = this.getSupport(extension);
			TransactionsFilteringDecorator filtered = new TransactionsFilteringDecorator(support,
					extensionCounters.supportCounts, extensionCounters.renaming);
			return new FlexibleDataset(extensionCounters, filtered);
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
