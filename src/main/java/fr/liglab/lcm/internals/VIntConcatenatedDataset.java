package fr.liglab.lcm.internals;

import fr.liglab.lcm.util.CopyIteratorDecorator;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.omg.CORBA.IntHolder;

/**
 * Here all transactions are prefixed by their length and concatenated in a
 * single int[]
 * 
 * This dataset internally performs - basic reduction : transactions contain
 * only items having a support count in [minusp, 100% [ - occurrence delivery :
 * occurrences are stored as indexes in the concatenated array - fast
 * prefix-preserving test (see inner class CandidatesIterator)
 */
public class VIntConcatenatedDataset extends Dataset {

	protected final byte[] concatenated;
	protected final int coreItem;
	protected final int transactionsCount;
	protected final int[] frequentItems;

	/**
	 * frequent item => array of occurrences indexes in "concatenated"
	 * Transactions are added in the same order in all occurences-arrays. This
	 * property is used in CandidatesIterator's prefix-preserving test
	 */
	protected final TIntObjectHashMap<TIntArrayList> occurrences = new TIntObjectHashMap<TIntArrayList>();

	/**
	 * Initial dataset constructor
	 * 
	 * "transactions" iterator will be traversed only once. Though, references
	 * to provided transactions will be kept and re-used during instanciation.
	 * None will be kept after.
	 */
	public VIntConcatenatedDataset(final int minimumsupport,
			final Iterator<int[]> transactions) {
		// in initial dataset, all items are candidate => all items < coreItem
		this.coreItem = Integer.MAX_VALUE;
		this.minsup = minimumsupport;

		CopyIteratorDecorator<int[]> transactionsCopier = new CopyIteratorDecorator<int[]>(
				transactions);
		this.genSupportCounts(transactionsCopier);
		this.transactionsCount = transactionsCopier.size();

		int remainingItemsCount = genClosureAndFilterCount();
		int remainingItemsSize = 0;
		for (TIntIntIterator count = supportCounts.iterator(); count.hasNext();) {
			count.advance();
			remainingItemsSize += count.value() * getVIntSize(count.key());
		}
		this.prepareOccurences();
		// over estimating the space taken by transaction sizes ... if we had a
		// better bound we could make it lower. I think 2 bytes are already a
		// lot (4k items in transaction)
		this.concatenated = new byte[remainingItemsSize + 5
				* this.transactionsCount];

		this.filter(transactionsCopier);

		this.frequentItems = occurrences.keys();
		Arrays.sort(this.frequentItems);
	}

	protected void filter(final Iterable<int[]> transactions) {
		TIntSet retained = this.supportCounts.keySet();
		IntHolder concIndex = new IntHolder(0);
		for (int[] transaction : transactions) {
			int startAt = concIndex.value;
			for (int item : transaction) {
				if (retained.contains(item)) {
					writeVInt(this.concatenated, concIndex, item);
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
					this.occurrences.get(readVInt(this.concatenated, addIndex))
							.add(newTransId);
				}
			}
		}
	}

	protected VIntConcatenatedDataset(VIntConcatenatedDataset parent,
			int extension) {
		this.supportCounts = new TIntIntHashMap();
		this.minsup = parent.minsup;
		this.coreItem = extension;

		TIntArrayList extOccurrences = parent.occurrences.get(extension);
		this.transactionsCount = extOccurrences.size();

		TIntIterator iterator = extOccurrences.iterator();
		while (iterator.hasNext()) {
			int tid = iterator.next();
			IntHolder transactionIndex = new IntHolder(tid);
			int length = readVInt(parent.concatenated, transactionIndex);
			transactionIndex.value = tid - length;
			while (transactionIndex.value < tid) {
				this.supportCounts.adjustOrPutValue(
						readVInt(parent.concatenated, transactionIndex), 1, 1);
			}
		}

		supportCounts.remove(extension);
		int remainingItemsCount = genClosureAndFilterCount();
		int remainingItemsSize = 0;
		for (TIntIntIterator count = supportCounts.iterator(); count.hasNext();) {
			count.advance();
			remainingItemsSize += count.value() * getVIntSize(count.key());
		}

		this.prepareOccurences();

		TIntSet kept = this.supportCounts.keySet();
		// over estimating the space taken by transaction sizes ... if we had a
		// better bound we could make it lower. I think 2 bytes are already a
		// lot (4k items in transaction)
		this.concatenated = new byte[remainingItemsSize + 4
				* this.transactionsCount];

		filterParent(parent.concatenated, extOccurrences.iterator(), kept);

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
	protected void filterParent(byte[] parent, TIntIterator occIterator,
			TIntSet keeped) {
		IntHolder concIndex = new IntHolder(0);
		while (occIterator.hasNext()) {
			int parentTid = occIterator.next();
			IntHolder parentIterator = new IntHolder(parentTid);
			int parentLength = readVInt(parent, parentIterator);
			parentIterator.value = parentTid - parentLength;
			int startAt = concIndex.value;
			while (parentIterator.value < parentTid) {
				int item = readVInt(parent, parentIterator);
				if (keeped.contains(item)) {
					writeVInt(this.concatenated, concIndex, item);
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
					this.occurrences.get(readVInt(this.concatenated, addIndex))
							.add(newTransId);
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
			this.occurrences.put(counts.key(),
					new TIntArrayList(counts.value()));
		}
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
	public VIntConcatenatedDataset getProjection(int extension) {
		return new VIntConcatenatedDataset(this, extension);
	}

	@Override
	public int getTransactionsCount() {
		return transactionsCount;
	}

	public int[] getSortedFrequents() {
		return frequentItems;
	}

	@Override
	public ExtensionsIterator getCandidatesIterator() {
		return new CandidatesIterator();
	}

	static public int readVInt(byte[] array, IntHolder pointer) {
		byte b = array[pointer.value];
		pointer.value++;
		if (b >= 0) {
			return (int) b;
		} else {
			int res = (b & 0x7F);
			int shift = 7;
			while (true) {
				b = array[pointer.value];
				pointer.value++;
				if (b > 0) {
					// this is the last b
					// TODO see if we can do a set byte directly
					res = res | (b << shift);
					break;
				} else {
					// make sure 0xO8 is seen as a byte
					res = res | ((b & 0x7F) << shift);
					shift += 7;
				}
			}
			return res;
		}
	}

	static public void writeVInt(byte[] array, IntHolder pointer, int value) {
		while (true) {
			if (value >= 0 && value < 0x00000080) {
				array[pointer.value] = (byte) value;
				// System.out.println("encoding "
				// + String.format("%X", (byte) value));
				pointer.value++;
				break;
			} else {
				// System.out.println("encoding "
				// + String.format("%X", ((byte) value)) + " into "
				// + String.format("%X", (((byte) value) | 0x80)));
				array[pointer.value] = (byte) (((byte) value) | 0x80);
				value = value >>> 7;
				pointer.value++;
			}
		}
	}

	static public int getVIntSize(int val) {
		if (val < 0) {
			return 5;
		} else if (val < 0x0000007F) {
			return 1;
		} else if (val < 0x00003FFF) {
			return 2;
		} else if (val < 0x00001FFFFF) {
			return 3;
		} else if (val < 0x0FFFFFFF) {
			return 4;
		} else {
			return 5;
		}
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

	public static void main(String[] args) {
		IntHolder ih = new IntHolder(0);
		byte[] tab = new byte[5];
		int val = 127;
		System.out.println(String.format("%X", val));
		System.out.println(val);
		writeVInt(tab, ih, val);
		for (byte b : tab) {
			System.out.print(String.format("%X", b));
		}
		System.out.println();
		System.out.println(ih.value);
		ih.value = 0;
		System.out.println(readVInt(tab, ih));
		System.out.println(ih.value);
	}
}
