package fr.liglab.lcm.internals;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import fr.liglab.lcm.util.CopyIteratorDecorator;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;


public class ConcatenatedCompressedDataset extends Dataset {
	/**
	 * Support list will contain transactions with 0 frequency because
	 * compression occurs AFTER their construction
	 */
	protected final int[] concatenated;
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
	 * @throws DontExploreThisBranchException 
	 */
	public ConcatenatedCompressedDataset(final int minimumsupport,
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
		this.concatenated = new int[remainingItemsCount + 2
				* this.transactionsCount];
		
		TIntArrayList transactionsList = this.filter(transactionsCopier);
		
		this.compress(transactionsList);
		
		this.frequentItems = occurrences.keys();
		Arrays.sort(this.frequentItems);
	}

	protected TIntArrayList filter(final Iterable<int[]> transactions) {
		TIntArrayList transactionsList = new TIntArrayList(
				this.transactionsCount);
		TIntSet retained = this.supportCounts.keySet();
		int i = 2;
		int tIndex = 1;

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
				transactionsList.add(tIndex);
				this.concatenated[tIndex] = length;
				this.concatenated[tIndex - 1] = 1;
				Arrays.sort(this.concatenated, tIndex + 1, i);
				i++;
				tIndex = i;
				i++;
			}
		}
		return transactionsList;
	}

	protected ConcatenatedCompressedDataset(
			ConcatenatedCompressedDataset parent, int extension) throws DontExploreThisBranchException {
		
		super(parent.minsup, extension);
		this.supportCounts = new TIntIntHashMap();
		TIntArrayList extOccurrences = parent.occurrences.get(extension);
		TIntIterator iterator = extOccurrences.iterator();
		int validTransactionEntries = 0;
		int tempCount = 0;
		while (iterator.hasNext()) {
			int tid = iterator.next();
			int length = parent.concatenated[tid];
			int frequency = parent.concatenated[tid - 1];
			tempCount += frequency;
			if (frequency > 0) {
				for (int i = tid + 1; i <= tid + length; i++) {
					if (parent.concatenated[i] >= 0) {
						validTransactionEntries += frequency;
						this.supportCounts.adjustOrPutValue(
								parent.concatenated[i], frequency, frequency);
					}
				}
			}
		}
		this.transactionsCount = tempCount;
		validTransactionEntries -= supportCounts.remove(extension);
		int remainingItemsCount = genClosureAndFilterCount();
		this.prepareOccurences();

		TIntSet kept = this.supportCounts.keySet();
		this.concatenated = new int[Math.min(remainingItemsCount,
				validTransactionEntries) + 2 * extOccurrences.size()];

		TIntArrayList transactionsList = filterParent(parent.concatenated,
				extOccurrences, kept);
		this.compress(transactionsList);
		this.frequentItems = this.occurrences.keys();
		Arrays.sort(this.frequentItems);
	}

	/**
	 * @param parent
	 * @param occIterator
	 *            iterator on occurences iterator (giving indexes in parent)
	 * @param kept
	 *            items that will remain in our transactions
	 */
	protected TIntArrayList filterParent(int[] parent, TIntArrayList occ,
			TIntSet kept) {
		TIntArrayList transactionsList = new TIntArrayList(occ.size());
		int i = 2;
		int tIndex = 1;
		final TIntIterator occIterator = occ.iterator();
		while (occIterator.hasNext()) {
			int parentTid = occIterator.next();
			int parentLength = parent[parentTid];
			int parentFreq = parent[parentTid - 1];
			if (parentFreq > 0) {
				int length = 0;

				for (int j = parentTid + 1; j <= parentTid + parentLength; j++) {
					int item = parent[j];
					if (item >= 0) {
						if (kept.contains(item)) {
							this.concatenated[i] = item;
							this.occurrences.get(item).add(tIndex);
							length++;
							i++;
						}
					}
				}

				if (length > 0) {
					transactionsList.add(tIndex);
					this.concatenated[tIndex] = length;
					this.concatenated[tIndex - 1] = parentFreq;
					i++;
					tIndex = i;
					i++;
				}
			}
		}
		return transactionsList;
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

	private void compress(TIntArrayList transactionsList) {
		if (!transactionsList.isEmpty()) {
			ArrayList<Integer> sortedList = new ArrayList<Integer>(
					transactionsList.size());
			TIntIterator iter = transactionsList.iterator();
			while (iter.hasNext()) {
				sortedList.add(iter.next());
			}
			Collections.sort(sortedList, new Comparator<Integer>() {
				
				public int compare(Integer t1, Integer t2) {
					if (concatenated[t1 - 1] == 0 || concatenated[t2 - 1] == 0) {
						throw new RuntimeException(
								"transaction with frequency 0, should have been eliminated");
					} else {
						int t1length = concatenated[t1];
						int t2length = concatenated[t2];
						int iterationLength = Math.min(t1length, t2length);
						for (int i = 1; i <= iterationLength; i++) {
							if (concatenated[t1 + i] < concatenated[t2 + i]) {
								return -1;
							} else if (concatenated[t1 + i] > concatenated[t2
									+ i]) {
								return 1;
							}
						}
						return t1length - t2length;
					}
				}
			});
			int currentRef = sortedList.get(0);
			int currentRefLength = concatenated[currentRef];
			for (int i = 1; i < sortedList.size(); i++) {
				int trans = sortedList.get(i);
				int transLength = concatenated[trans];
				if (transLength == 0) {
					break;
				}
				int pointerCurrentRef = 1;
				int pointerTrans = 1;
				final int prefixEnd = Integer.MAX_VALUE;
				while (true) {
					if (pointerCurrentRef > currentRefLength) {
						if (pointerTrans > transLength
								|| this.concatenated[trans + pointerTrans] > prefixEnd) {
							// prefix comparison finished, we already took care
							// of
							// the suffix of the reference, and we discard the
							// rest
							// of the suffix of trans
							this.concatenated[currentRef - 1] += this.concatenated[trans - 1];
							this.concatenated[trans - 1] = 0;
							break;
						} else {
							// prefix comparison not finished, so different
							// prefix,
							// switch to next
							currentRef = trans;
							currentRefLength = transLength;
							break;
						}
					} else if (pointerTrans > transLength) {
						if (this.concatenated[currentRef + pointerCurrentRef] > prefixEnd) {
							// same prefix, but all remaining suffix goes away
							Arrays.fill(this.concatenated, -1, currentRef
									+ pointerCurrentRef, currentRef
									+ currentRefLength + 1);
							this.concatenated[currentRef - 1] += this.concatenated[trans - 1];
							this.concatenated[trans - 1] = 0;
							break;
						} else {
							// prefix comparison not finished, so different
							// prefix,
							// switch to next
							currentRef = trans;
							currentRefLength = transLength;
							break;
						}
					} else if (concatenated[currentRef + pointerCurrentRef] == concatenated[trans
							+ pointerTrans]) {
						// same value, doesn't matter if we're in prefix or
						// suffix,
						// go on
						pointerTrans++;
						do {
							pointerCurrentRef++;
						} while (pointerCurrentRef <= currentRefLength
								&& this.concatenated[currentRef
										+ pointerCurrentRef] == -1);
					} else if (concatenated[currentRef + pointerCurrentRef] < prefixEnd
							|| concatenated[trans + pointerTrans] < prefixEnd) {
						// prefix comparison not finished, so different prefix,
						// switch to next
						currentRef = trans;
						currentRefLength = transLength;
						break;
					}
					// from that point, suffix match, so they will be merged
					// eventually
					else if (concatenated[currentRef + pointerCurrentRef] > concatenated[trans
							+ pointerTrans]) {
						// skip the suffix of trans that is not in ref
						do {
							pointerTrans++;
						} while (pointerTrans <= transLength
								&& this.concatenated[trans + pointerTrans] < this.concatenated[currentRef
										+ pointerCurrentRef]);
					} else if (concatenated[currentRef + pointerCurrentRef] < concatenated[trans
							+ pointerTrans]) {
						// dismiss the suffix of ref which is not in trans
						do {
							this.concatenated[currentRef + pointerCurrentRef] = -1;
							pointerCurrentRef++;
						} while (pointerCurrentRef <= currentRefLength
								&& this.concatenated[currentRef
										+ pointerCurrentRef] < concatenated[trans
										+ pointerTrans]);
					}
				}
			}
		}
	}

	/**
	 * @return true if there is no int j > candidate having the same support as
	 *         candidate at the given index
	 */
	private int ppTest(final int candidateIndex) {
		final int candidate = frequentItems[candidateIndex];
		final int candidateSupport = supportCounts.get(candidate);
		TIntArrayList candidateOccurrences = occurrences.get(candidate);

		for (int i = frequentItems.length - 1; i > candidateIndex ; i--) {
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
	public Dataset getProjection(int extension) throws DontExploreThisBranchException {
		return new ConcatenatedCompressedDataset(this, extension);
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

		public int getExtension() throws DontExploreThisBranchException {
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
				} else {
					int parent = ppTest(next_index_local);
					if (parent == -1) {
						return frequentItems[next_index_local];
					} else {
						throw new DontExploreThisBranchException(frequentItems[next_index_local], parent);
					}
				}
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (this.concatenated.length > 3) {
			int index = 0;
			while (index + 1 < this.concatenated.length) {
				int freq = this.concatenated[index];
				index++;
				int length = this.concatenated[index];
				if (length == 0) {
					index--;
					while (index < this.concatenated.length) {
						sb.append(this.concatenated[index] + " ");
						index++;
					}
				} else {
					sb.append("freq " + freq + " length " + length);
					for (int i = 0; i < length; i++) {
						index++;
						sb.append(" " + this.concatenated[index]);
					}
					sb.append("\n");
					index++;
				}
			}
		}
		return sb.toString();
	}
}
