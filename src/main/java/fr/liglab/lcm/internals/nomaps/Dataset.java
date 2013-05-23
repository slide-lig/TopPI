package fr.liglab.lcm.internals.nomaps;

import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

import fr.liglab.lcm.internals.TransactionReader;
import fr.liglab.lcm.internals.tidlist.ConsecutiveItemsConcatenatedTidList;
import fr.liglab.lcm.internals.tidlist.ConsecutiveItemsShortConcatenatedTidList;
import fr.liglab.lcm.internals.tidlist.TidList;
import fr.liglab.lcm.internals.tidlist.TidList.TIntIterable;
import fr.liglab.lcm.internals.transactions.ConcatenatedTransactionsList;
import fr.liglab.lcm.internals.transactions.ShortConcatenatedTransactionsList;
import fr.liglab.lcm.internals.transactions.TransactionsList;
import fr.liglab.lcm.internals.transactions.TransactionsWriter;
import gnu.trove.iterator.TIntIterator;

/**
 * Stores transactions and does occurrence delivery
 */
public class Dataset {
	
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
	
	/**
	 * @param counters
	 * @param transactions assumed to be filtered according to counters
	 */
	Dataset(Counters counters, final Iterator<TransactionReader> transactions) {
		
		int maxTransId;
		
		if (ShortConcatenatedTransactionsList.compatible(counters)) {
			this.transactions = new ShortConcatenatedTransactionsList(
					counters.distinctTransactionLengthSum, counters.distinctTransactionsCount);
			
			maxTransId = ShortConcatenatedTransactionsList.getMaxTransId(counters);
		} else {
			this.transactions = new ConcatenatedTransactionsList(
					counters.distinctTransactionLengthSum, counters.distinctTransactionsCount);
			
			maxTransId = ConcatenatedTransactionsList.getMaxTransId(counters);
		}
		
		if (ConsecutiveItemsShortConcatenatedTidList.compatible(maxTransId)) {
			this.tidLists = new ConsecutiveItemsShortConcatenatedTidList(counters.supportCounts);
		} else {
			this.tidLists = new ConsecutiveItemsConcatenatedTidList(counters.supportCounts);
		}
		
		TransactionsWriter writer = this.transactions.getWriter();
		while (transactions.hasNext()) {
			TransactionReader transaction = transactions.next();
			
			if (transaction.hasNext()) {
				final int transId = writer.beginTransaction(transaction.getTransactionSupport());
				
				while (transaction.hasNext()) {
					final int item = transaction.next();
					writer.addItem(item);
					this.tidLists.addTransaction(item, transId);
				}
				
				writer.endTransaction();
			}
		}
	}
	
	/**
	 * In this implementation the inputted transactions are assumed to be filtered, therefore it 
	 * returns null. However this is not true for subclasses.
	 * @return items known to have a 100% support in this dataset
	 */
	int[] getIgnoredItems() {
		return null; // we assume this class always receives 
	}
	
	/**
	 * @return how many transactions (ignoring their weight) are stored behind this dataset
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
		
		public TransactionsIterator(TIntIterator tids) {
			this.it = tids;
		}
		
		@Override
		public void remove() {
			throw new NotImplementedException();
		}

		@Override
		public TransactionReader next() {
			return transactions.get(this.it.next());
		}
		
		@Override
		public boolean hasNext() {
			return this.it.hasNext();
		}
	}
}
