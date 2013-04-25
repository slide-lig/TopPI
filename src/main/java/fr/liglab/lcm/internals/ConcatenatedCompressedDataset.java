package fr.liglab.lcm.internals;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

/**
 * In this dataset (which does implement occurrence-delivery) transactions are 
 * concatenated in a single int[] and prefixed by their length and weight
 * 
 * /!\ it assumes transactions'items are sorted in increasing order  
 * 
 * It is made for dense datasets and overrides ppTest
 * 
 * It implements complete database reduction, as describe by Uno et al. in UnoAUA04 section 5.2
 */
public class ConcatenatedCompressedDataset extends Dataset {
	
	protected final DatasetCounters counters; // TODO find a way to put this in Dataset without getting boring
	
	/**
	 * Support list will contain transactions with 0 frequency because
	 * compression occurs AFTER their construction
	 */
	protected final int[] concatenated;

	/**
	 * frequent item => array of occurrences indexes in "concatenated"
	 * Transactions are added in the same order in all occurences-arrays. This
	 * property is used in prefix-preserving test
	 */
	protected final TIntObjectHashMap<TIntArrayList> occurrences = new TIntObjectHashMap<TIntArrayList>();

	/**
	 * Compression is inefficient if you don't give a coreItem, this constructor 
	 * is actually for testing purposes 
	 * @param counts results from a first pass over transactions
	 * @param transactions , assuming items are sorted in increasing order
	 */
	ConcatenatedCompressedDataset(final DatasetCounters counts, 
			final Iterator<TransactionReader> transactions) {
		
		this(counts, transactions, Integer.MAX_VALUE);
	}
	
	/**
	 * @param counts results from a first pass over transactions
	 * @param transactions , assuming items are sorted in increasing order
	 * @param coreItem when building a projection, compression 
	 */
	ConcatenatedCompressedDataset(final DatasetCounters counts, 
			final Iterator<TransactionReader> transactions, int coreItem) {
		
		this.counters = counts;
		
		TIntIntIterator supIterator = counts.supportCounts.iterator();
		while (supIterator.hasNext()) {
			supIterator.advance();
			this.occurrences.put(supIterator.key(), new TIntArrayList(supIterator.value()));
		}
		
		int tableLength = counts.itemsRead + 2*counts.transactionsCount;
		this.concatenated = new int[tableLength];
		
		TIntArrayList transactionsList = new TIntArrayList(counts.transactionsCount);
		int i = 2;
		int tIndex = 1;

		while (transactions.hasNext()) {
			int length = 0;
			
			TransactionReader transaction = transactions.next();
			while (transaction.hasNext()) {
				int item = transaction.next();
				this.concatenated[i] = item;
				this.occurrences.get(item).add(tIndex);
				length++;
				i++;
			}

			if (length > 0) {
				transactionsList.add(tIndex);
				this.concatenated[tIndex] = length;
				this.concatenated[tIndex - 1] = transaction.getTransactionSupport();
				i++;
				tIndex = i;
				i++;
			}
		}
		
		this.compress(transactionsList, coreItem);
	}
	
	private void compress(TIntArrayList transactionsList, int coreItem) {
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
				final int prefixEnd = coreItem;
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
	
	

	@Override
	public Iterator<TransactionReader> getSupport(int item) {
		TIntIterator it = this.occurrences.get(item).iterator();
		return new ConcatenatedTransactionsReader(this.concatenated, it, true);
	}

	@Override
	public final DatasetCounters getCounters() {
		return this.counters;
	}

	@Override
	Dataset project(int extension, DatasetCounters extensionCounters) {
		Iterator<TransactionReader> support = this.getSupport(extension);
		TransactionsFilteringDecorator filtered = 
				new TransactionsFilteringDecorator(support, extensionCounters.getFrequents());
		return new ConcatenatedCompressedDataset(extensionCounters, filtered, extension);
	}
	
	
	
	@Override
	public int ppTest(int extension) {
		int index = Arrays.binarySearch(this.counters.sortedFrequents, extension);
		return this.ppTest(index, this.counters.sortedFrequents, this.counters.supportCounts);
	}
	
	/**
	 * @return true if there is no int j > candidate having the same support as
	 *         candidate at the given index
	 */
	private int ppTest(final int candidateIndex, final int[] frequentItems, final TIntIntMap supportCounts) {
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
