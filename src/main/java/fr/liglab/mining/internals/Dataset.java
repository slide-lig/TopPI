package fr.liglab.mining.internals;

import java.util.Iterator;

import fr.liglab.mining.PLCM;
import fr.liglab.mining.PLCM.TopLCMCounters;
import fr.liglab.mining.internals.tidlist.IntConsecutiveItemsConcatenatedTidList;
import fr.liglab.mining.internals.tidlist.TidList;
import fr.liglab.mining.internals.tidlist.UShortConsecutiveItemsConcatenatedTidList;
import fr.liglab.mining.internals.tidlist.TidList.TIntIterable;
import fr.liglab.mining.internals.transactions.IntIndexedTransactionsList;
import fr.liglab.mining.internals.transactions.ReusableTransactionIterator;
import fr.liglab.mining.internals.transactions.TransactionsList;
import fr.liglab.mining.internals.transactions.TransactionsWriter;
import fr.liglab.mining.internals.transactions.UShortIndexedTransactionsList;
import gnu.trove.iterator.TIntIterator;

/**
 * Stores transactions and does occurrence delivery
 */
public class Dataset implements Cloneable {

	protected final TransactionsList transactions;

	/**
	 * frequent item => array of occurrences indexes in "concatenated"
	 * Transactions are added in the same order in all occurrences-arrays.
	 */
	protected final TidList tidLists;

	protected Dataset(TransactionsList transactions, TidList occurrences) {
		this.transactions = transactions;
		this.tidLists = occurrences;
	}

	@Override
	protected Dataset clone() {
		return new Dataset(this.transactions.clone(), this.tidLists.clone());
	}

	Dataset(Counters counters, final Iterator<TransactionReader> transactions) {
		this(counters, transactions, Integer.MAX_VALUE);
	}

	/**
	 * @param counters
	 * @param transactions
	 *            assumed to be filtered according to counters
	 * @param tidListBound
	 *            - highest item (exclusive) which will have a tidList. set to
	 *            MAX_VALUE when using predictive pptest.
	 */
	Dataset(Counters counters, final Iterator<TransactionReader> transactions, int tidListBound) {

		int maxTransId;

		// if (UByteIndexedTransactionsList.compatible(counters)) {
		// this.transactions = new UByteIndexedTransactionsList(counters);
		// maxTransId = UByteIndexedTransactionsList.getMaxTransId(counters);
		// } else
		if (UShortIndexedTransactionsList.compatible(counters)) {
			this.transactions = new UShortIndexedTransactionsList(counters);
			maxTransId = UShortIndexedTransactionsList.getMaxTransId(counters);
		} else {
			this.transactions = new IntIndexedTransactionsList(counters);
			maxTransId = IntIndexedTransactionsList.getMaxTransId(counters);
		}

		// if (UByteConsecutiveItemsConcatenatedTidList.compatible(maxTransId))
		// {
		// this.tidLists = new
		// UByteConsecutiveItemsConcatenatedTidList(counters, tidListBound);
		// } else
		if (UShortConsecutiveItemsConcatenatedTidList.compatible(maxTransId)) {
			this.tidLists = new UShortConsecutiveItemsConcatenatedTidList(counters, tidListBound);
		} else {
			this.tidLists = new IntConsecutiveItemsConcatenatedTidList(counters, tidListBound);
		}

		TransactionsWriter writer = this.transactions.getWriter();
		while (transactions.hasNext()) {
			TransactionReader transaction = transactions.next();
			if (transaction.getTransactionSupport() != 0 && transaction.hasNext()) {
				final int transId = writer.beginTransaction(transaction.getTransactionSupport());

				while (transaction.hasNext()) {
					final int item = transaction.next();
					writer.addItem(item);

					if (item < tidListBound) {
						this.tidLists.addTransaction(item, transId);
					}
				}

				writer.endTransaction();
			}
		}
	}

	public void compress(int coreItem) {
		((PLCM.TopLCMThread) Thread.currentThread()).counters[TopLCMCounters.TransactionsCompressions.ordinal()]++;
		this.transactions.compress(coreItem);
	}

	/**
	 * In this implementation the inputted transactions are assumed to be
	 * filtered, therefore it returns null. However this is not true for
	 * subclasses.
	 * 
	 * @return items known to have a 100% support in this dataset
	 */
	int[] getIgnoredItems() {
		return null; // we assume this class always receives
	}

	/**
	 * @return how many transactions (ignoring their weight) are stored behind
	 *         this dataset
	 */
	int getStoredTransactionsCount() {
		return this.transactions.size();
	}

	public TransactionsIterable getSupport(int item) {
		return new TransactionsIterable(this.tidLists.getIterable(item));
	}

	public final class TransactionsIterable implements Iterable<TransactionReader> {
		final TIntIterable tids;

		public TransactionsIterable(TIntIterable tidList) {
			this.tids = tidList;
		}

		@Override
		public Iterator<TransactionReader> iterator() {
			return new TransactionsIterator(this.tids.iterator());
		}
	}

	protected final class TransactionsIterator implements Iterator<TransactionReader> {

		protected final TIntIterator it;
		private final ReusableTransactionIterator transIter;

		public TransactionsIterator(TIntIterator tids) {
			this.it = tids;
			this.transIter = transactions.getIterator();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public TransactionReader next() {
			this.transIter.setTransaction(this.it.next());
			return this.transIter;
		}

		@Override
		public boolean hasNext() {
			return this.it.hasNext();
		}
	}
}
