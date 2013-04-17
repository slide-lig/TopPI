package fr.liglab.lcm.internals;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import fr.liglab.lcm.util.CopyIteratorDecorator;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;

import java.util.Arrays;
import java.util.Iterator;

public abstract class FilteredDataset extends IterableDataset {
	protected int transactionsCount;// number of valid transactions read from
									// source
	/**
	 * frequent item => array of occurrences indexes in "concatenated"
	 * Transactions are added in the same order in all occurences-arrays. This
	 * property is used in CandidatesIterator's prefix-preserving test
	 */
	final TIntObjectHashMap<TIntArrayList> occurrences = new TIntObjectHashMap<TIntArrayList>();

	public FilteredDataset(final int minimumsupport, final Iterator<int[]> transactions)
			throws DontExploreThisBranchException {

		// in initial dataset, all items are candidate => all items < coreItem
		super(minimumsupport, Integer.MAX_VALUE);

		CopyIteratorDecorator<int[]> transactionsCopier = new CopyIteratorDecorator<int[]>(transactions);
		this.genSupportCounts(transactionsCopier);
		this.transactionsCount = transactionsCopier.size();

		int sumOfRemainingItemsSupport = genClosureAndFilterCount();
		this.prepareOccurences();
		this.prepareTransactionsStructure(sumOfRemainingItemsSupport, sumOfRemainingItemsSupport,
				this.transactionsCount);

		this.filter(transactionsCopier);

		this.frequentItems = occurrences.keys();
		Arrays.sort(this.frequentItems);
	}

	/**
	 * extensionTids may be null - if it's not, it must contain indexes in
	 * parent's concatenated field
	 * 
	 * @throws DontExploreThisBranchException
	 */
	public FilteredDataset(IterableDataset parent, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException {

		super(parent.minsup, extension);
		this.supportCounts = new TIntIntHashMap();

		TIntList extOccurrences = parent.getTidList(extension);
		this.transactionsCount = 0;
		int distinctTransactionsCount = 0;
		int distinctTransactionsLength = 0;
		TIntIterator iterator = extOccurrences.iterator();
		while (iterator.hasNext()) {
			int tid = iterator.next();
			TransactionReader parentIterator = parent.readTransaction(tid);
			int sup = parentIterator.getTransactionSupport();
			if (sup > 0) {
				this.transactionsCount += sup;
				distinctTransactionsCount++;
				while (parentIterator.hasNext()) {
					distinctTransactionsLength++;
					this.supportCounts.adjustOrPutValue(parentIterator.next(), sup, sup);
				}
			}
		}

		supportCounts.remove(extension);

		if (ignoreItems != null) {
			for (int item : ignoreItems) {
				supportCounts.remove(item);
			}
		}

		int sumOfRemainingItemsSupport = genClosureAndFilterCount();
		this.prepareOccurences();

		TIntSet kept = this.supportCounts.keySet();
		this.prepareTransactionsStructure(sumOfRemainingItemsSupport, distinctTransactionsLength,
				distinctTransactionsCount);

		filterParent(parent, extOccurrences, kept);

		this.frequentItems = this.occurrences.keys();
		Arrays.sort(this.frequentItems);
	}

	protected FilteredDataset(IterableDataset parent, int extension) throws DontExploreThisBranchException {
		this(parent, extension, null);
	}

	protected abstract void prepareTransactionsStructure(int sumOfRemainingItemsSupport,
			int distinctTransactionsLength, int distinctTransactionsCount);

	protected void filter(final Iterable<int[]> transactions) {
		TIntSet retained = this.supportCounts.keySet();
		TransactionsWriter tw = this.getTransactionsWriter(false);
		for (int[] transaction : transactions) {
			boolean transactionExists = false;
			for (int item : transaction) {
				if (retained.contains(item)) {
					transactionExists = true;
					tw.addItem(item);
				}
			}
			if (transactionExists) {
				int tid = tw.endTransaction(1);
				TransactionReader read = this.readTransaction(tid);
				// no need to check for support, we just wrote it, it s one
				while (read.hasNext()) {
					this.occurrences.get(read.next()).add(tid);
				}
			}
		}
	}

	/**
	 * @param parent
	 * @param occIterator
	 *            iterator on occurences iterator (giving indexes in parent)
	 * @param kept
	 *            items that will remain in our transactions
	 */
	protected void filterParent(IterableDataset parent, TIntList occ, TIntSet kept) {
		TransactionsWriter tw = this.getTransactionsWriter(parent.itemsSorted());
		TIntIterator occIterator = occ.iterator();
		while (occIterator.hasNext()) {
			int parentTid = occIterator.next();
			TransactionReader parentIterator = parent.readTransaction(parentTid);
			if (parentIterator.getTransactionSupport() > 0) {
				boolean transactionExists = false;
				while (parentIterator.hasNext()) {
					int item = parentIterator.next();
					if (kept.contains(item)) {
						transactionExists = true;
						tw.addItem(item);
					}
				}
				if (transactionExists) {
					int tid = tw.endTransaction(parentIterator.getTransactionSupport());
					TransactionReader read = this.readTransaction(tid);
					if (tid >= 0) {
						while (read.hasNext()) {
							this.occurrences.get(read.next()).add(tid);
						}
					} else {
						TIntList tids = tw.getTids();
						while (read.hasNext()) {
							this.occurrences.get(read.next()).addAll(tids);
						}
					}
				}
			}
		}
	}

	/**
	 * Pre-instanciate occurrences ArrayLists according to this.supportCounts
	 */
	protected void prepareOccurences() {
		TIntIntIterator counts = this.supportCounts.iterator();
		while (counts.hasNext()) {
			counts.advance();
			this.occurrences.put(counts.key(), new TIntArrayList(counts.value()));
		}
	}

	@Override
	protected TIntList getTidList(int item) {
		return this.occurrences.get(item);
	}

	/**
	 * @return greatest j > candidate having the same support as candidate, -1
	 *         if not such item exists
	 */
	public int prefixPreservingTest(final int candidate) {
		int candidateIdx = Arrays.binarySearch(frequentItems, candidate);
		if (candidateIdx < 0) {
			throw new RuntimeException("Unexpected : prefixPreservingTest of an infrequent item, " + candidate);
		}

		return ppTest(candidateIdx);
	}

	/**
	 * @return greatest j > candidate having the same support as candidate at
	 *         the given index, -1 if not such item exists
	 */
	private int ppTest(final int candidateIndex) {
		final int candidate = frequentItems[candidateIndex];
		final int candidateSupport = supportCounts.get(candidate);
		TIntArrayList candidateOccurrences = occurrences.get(candidate);

		for (int i = frequentItems.length - 1; i > candidateIndex; i--) {
			int j = frequentItems[i];

			if (supportCounts.get(j) >= candidateSupport) {
				TIntArrayList jOccurrences = occurrences.get(j);
				if (isAincludedInB(candidateOccurrences, jOccurrences)) {
					return j;
				}
			}
		}

		return -1;
	}

	/**
	 * Assumptions : - both contain array indexes appended in increasing order -
	 * you already tested that B.size >= A.size
	 * 
	 * @return true if A is included in B
	 */
	public boolean isAincludedInB(final TIntArrayList a, final TIntArrayList b) {
		TIntIterator aIt = a.iterator();
		TIntIterator bIt = b.iterator();

		int tidA = 0;
		int tidB = 0;

		while (aIt.hasNext() && bIt.hasNext()) {
			tidA = aIt.next();
			tidB = bIt.next();

			while (tidB < tidA && bIt.hasNext()) {
				tidB = bIt.next();
			}

			if (tidB > tidA) {
				return false;
			}
		}

		return tidA == tidB && !aIt.hasNext();
	}

	@Override
	public int getTransactionsCount() {
		return transactionsCount;
	}

	public int[] getSortedFrequents() {
		return frequentItems;
	}

	public abstract int getRealSize();

	@Override
	public Dataset getProjection(int extension, int[] ignoreItems) throws DontExploreThisBranchException {
		double extensionSupport = this.supportCounts.get(extension);
		if ((extensionSupport / this.transactionsCount) > UnfilteredDataset.FILTERING_THRESHOLD) {
			return this.createUnfilteredDataset(this, extension, ignoreItems);
		} else {
			return this.createFilteredDataset(this, extension, ignoreItems);
		}
	}

	public abstract Dataset createUnfilteredDataset(FilteredDataset upper, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException;

	public abstract Dataset createFilteredDataset(FilteredDataset upper, int extension, int[] ignoreItems)
			throws DontExploreThisBranchException;

	protected abstract TransactionsWriter getTransactionsWriter(boolean sourceSorted);

	protected interface TransactionsWriter {

		public void addItem(int item);

		/*
		 * a negative result means there are several tids, so need to use
		 * getTids
		 */
		public int endTransaction(int support);

		public TIntList getTids();
	}

}
