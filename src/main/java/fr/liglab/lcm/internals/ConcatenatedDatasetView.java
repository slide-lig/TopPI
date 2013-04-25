package fr.liglab.lcm.internals;

import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;

import java.util.Iterator;

/**
 * Exploits transactions concatenated in another ConcatenatedDataset, 
 * keeping only those containing an item
 * 
 * It does NOT stores transactions' weights, nor it performs ppTest 
 * and occurrence delivery.
 */
class ConcatenatedDatasetView extends ConcatenatedDataset {
	static final double THRESHOLD = 0.15;
	
	protected final int[] ignoreItems; // items known to have a 100% support in parent.concatenated
	protected final ConcatenatedDataset parent;
	protected final TIntArrayList tids;
	
	protected TIntArrayList latestBuilt = null;
	protected int latestBuiltExtension = Integer.MIN_VALUE;
	
	ConcatenatedDatasetView(final DatasetCounters counts, ConcatenatedDataset upper, int extension) {
		super(counts, (upper instanceof ConcatenatedDatasetView) ? ((ConcatenatedDatasetView) upper).parent.concatenated : upper.concatenated);
		
		if (upper instanceof ConcatenatedDatasetView) {
			ConcatenatedDatasetView upperView = (ConcatenatedDatasetView) upper;
			this.parent = upperView.parent;
			this.tids = upperView.getExtensionTIDs(extension);
			this.ignoreItems = ItemsetsFactory.extend(counts.closure, extension, upperView.ignoreItems);
		} else {
			this.parent = upper;
			this.tids = this.parent.occurrences.get(extension);
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
		TIntIterator it = getExtensionTIDs(item).iterator();
		return new ConcatenatedTransactionsReader(this.concatenated, it, false);
	}
	
	/**
	 * buildExtensionTIDs with memoization : it's usually invoked twice
	 * for the same extension : once by getSupport and, likely, again by constructor
	 * 
	 * TODO verify "synchronized" use
	 */
	private synchronized TIntArrayList getExtensionTIDs(int extension) {
		if (this.latestBuiltExtension != extension) {
			this.latestBuilt = buildExtensionTIDs(extension);
			this.latestBuiltExtension = extension;
		}
		
		return this.latestBuilt;
	}
	
	
	/**
	 * assumes all theses TID-lists contain indexes in increasing order
	 * @return current Tids intersected with extension's occurrences
	 */
	private TIntArrayList buildExtensionTIDs(int extension) {
		TIntArrayList extensionTids = new TIntArrayList();
		
		TIntIterator myTidsIt = this.tids.iterator();
		TIntIterator parentExtTidsIt = this.parent.occurrences.get(extension).iterator();
		
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
}
