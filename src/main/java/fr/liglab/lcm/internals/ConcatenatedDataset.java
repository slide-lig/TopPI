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
import java.util.concurrent.atomic.AtomicInteger;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Here all transactions are prefixed by their length and concatenated in a
 * single int[]
 * 
 * This dataset internally performs - basic reduction : transactions contain
 * only items having a support count in [minusp, 100% [ - occurrence delivery :
 * occurrences are stored as indexes in the concatenated array - fast
 * prefix-preserving test (see inner class CandidatesIterator)
 */
public class ConcatenatedDataset extends IterableDataset {

	final int[] concatenated;

	protected final int transactionsCount;
	protected final int[] frequentItems;

	/**
	 * frequent item => array of occurrences indexes in "concatenated"
	 * Transactions are added in the same order in all occurences-arrays. This
	 * property is used in CandidatesIterator's prefix-preserving test
	 */
	final TIntObjectHashMap<TIntArrayList> occurrences = new TIntObjectHashMap<TIntArrayList>();

	/**
	 * Initial dataset constructor
	 * 
	 * "transactions" iterator will be traversed only once. Though, references
	 * to provided transactions will be kept and re-used during instanciation.
	 * None will be kept after.
	 * 
	 * @throws DontExploreThisBranchException
	 */
	public ConcatenatedDataset(final int minimumsupport,
			final Iterator<int[]> transactions)
			throws DontExploreThisBranchException {

		// in initial dataset, all items are candidate => all items < coreItem
		super(minimumsupport, Integer.MAX_VALUE);

		CopyIteratorDecorator<int[]> transactionsCopier = new CopyIteratorDecorator<int[]>(
				transactions);
		this.genSupportCounts(transactionsCopier);
		this.transactionsCount = transactionsCopier.size();

		int remainingItemsCount = genClosureAndFilterCount();
		this.prepareOccurences();
		this.concatenated = new int[remainingItemsCount
				+ this.transactionsCount];

		this.filter(transactionsCopier);

		this.frequentItems = occurrences.keys();
		Arrays.sort(this.frequentItems);
	}

	protected void filter(final Iterable<int[]> transactions) {
		TIntSet retained = this.supportCounts.keySet();
		int i = 1;
		int tIndex = 0;

		for (int[] transaction : transactions) {
			int length = 0;

			for (int item : transaction) {
				if (retained.contains(item)) {
					this.concatenated[i] = item;
					this.occurrences.get(item).add(tIndex);
					length++;
					i++;
				}
			}

			if (length > 0) {
				this.concatenated[tIndex] = length;
				tIndex = i;
				i++;
			}
		}
	}

	protected ConcatenatedDataset(IterableDataset parent, int extension)
			throws DontExploreThisBranchException {
		this(parent, extension, null);
	}

	/**
	 * extensionTids may be null - if it's not, it must contain indexes in
	 * parent's concatenated field
	 * 
	 * @throws DontExploreThisBranchException
	 */
	public ConcatenatedDataset(IterableDataset parent, int extension,
			int[] ignoreItems) throws DontExploreThisBranchException {

		super(parent.minsup, extension);
		this.supportCounts = new TIntIntHashMap();

		TIntList extOccurrences = parent.getTidList(extension);
		this.transactionsCount = extOccurrences.size();

		TIntIterator iterator = extOccurrences.iterator();
		while (iterator.hasNext()) {
			int tid = iterator.next();
			TIntIterator parentIterator = parent.readTransaction(tid);
			while (parentIterator.hasNext()) {
				this.supportCounts
						.adjustOrPutValue(parentIterator.next(), 1, 1);
			}
		}

		supportCounts.remove(extension);

		if (ignoreItems != null) {
			for (int item : ignoreItems) {
				supportCounts.remove(item);
			}
		}

		int remainingItemsCount = genClosureAndFilterCount();
		this.prepareOccurences();

		TIntSet kept = this.supportCounts.keySet();
		this.concatenated = new int[remainingItemsCount
				+ this.transactionsCount];

		filterParent(parent, extOccurrences.iterator(), kept);

		this.frequentItems = this.occurrences.keys();
		Arrays.sort(this.frequentItems);
	}

	/**
	 * @param parent
	 * @param occIterator
	 *            iterator on occurences iterator (giving indexes in parent)
	 * @param keeped
	 *            items that will remain in our transactions
	 */
	protected void filterParent(IterableDataset parent,
			TIntIterator occIterator, TIntSet keeped) {
		int i = 1;
		int tIndex = 0;

		while (occIterator.hasNext()) {
			int parentTid = occIterator.next();
			TIntIterator parentIterator = parent.readTransaction(parentTid);
			int length = 0;
			while (parentIterator.hasNext()) {
				int item = parentIterator.next();

				if (keeped.contains(item)) {
					this.concatenated[i] = item;
					this.occurrences.get(item).add(tIndex);
					length++;
					i++;
				}
			}

			if (length > 0) {
				this.concatenated[tIndex] = length;
				tIndex = i;
				i++;
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
			this.occurrences.put(counts.key(),
					new TIntArrayList(counts.value()));
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
			throw new RuntimeException(
					"Unexpected : prefixPreservingTest of an infrequent item, "
							+ candidate);
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
	public Dataset getProjection(int extension)
			throws DontExploreThisBranchException {

		double extensionSupport = this.supportCounts.get(extension);

		if ((extensionSupport / this.transactionsCount) > ConcatenatedUnfilteredDataset.FILTERING_THRESHOLD) {
			return new ConcatenatedUnfilteredDataset(this, extension);
		} else {
			return new ConcatenatedDataset(this, extension);
		}
	}

	@Override
	public int getTransactionsCount() {
		return transactionsCount;
	}

	public int[] getSortedFrequents() {
		return frequentItems;
	}

	public int getRealSize() {
		return concatenated.length;
	}

	@Override
	public ExtensionsIterator getCandidatesIterator() {
		return new CandidatesIterator();
	}

	@Override
	protected TIntIterator readTransaction(int tid) {
		return new ItemsIterator(tid);
	}

	/**
	 * Iterates on candidates items such that - their support count is in
	 * [minsup, transactionsCount[ , - candidate < coreItem - no item >
	 * candidate has the same support as candidate (aka fast-prefix-preservation
	 * test) => assuming items from previously found patterns (including
	 * coreItem) have been removed !! coreItem = extension item (if it exists)
	 */
	protected class CandidatesIterator implements ExtensionsIterator {
		private AtomicInteger next_index;
		private final int candidatesLength; // candidates is
											// frequentItems[0:candidatesLength[

		public int[] getSortedFrequents() {
			return frequentItems;
		}

		/**
		 * @param original
		 *            an iterator on frequent items
		 * @param min
		 */
		public CandidatesIterator() {
			this.next_index = new AtomicInteger(-1);

			int coreItemIndex = Arrays.binarySearch(frequentItems, coreItem);
			if (coreItemIndex >= 0) {
				throw new RuntimeException(
						"Unexpected : coreItem appears in frequentItems !");
			}

			// binarySearch returns -(insertion_point)-1
			// where insertion_point == index of first element greater OR
			// a.length
			candidatesLength = -coreItemIndex - 1;
		}

		public int getExtension() {
			if (candidatesLength < 0) {
				return -1;
			}
			while (true) {
				int next_index_local = this.next_index.incrementAndGet();
				if (next_index_local < 0) {
					// overflow, just in case
					return -1;
				}
				if (next_index_local >= this.candidatesLength) {
					return -1;
				} else { // if (ppTest(next_index_local)) {
					return frequentItems[next_index_local];
				}
			}
		}
	}

	private class ItemsIterator implements TIntIterator {
		private int max;
		private int index;

		public ItemsIterator(int tid) {
			int length = concatenated[tid];
			this.max = tid + length;
			this.index = tid + 1;
		}

		@Override
		public boolean hasNext() {
			return this.index != this.max;
		}

		@Override
		public void remove() {
			throw new NotImplementedException();
		}

		@Override
		public int next() {
			int val = concatenated[this.index];
			this.index++;
			return val;
		}

	}
}
