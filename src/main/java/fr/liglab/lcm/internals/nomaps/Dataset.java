package fr.liglab.lcm.internals.nomaps;

import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

import fr.liglab.lcm.internals.TransactionReader;
import fr.liglab.lcm.internals.tidlist.ConsecutiveItemsConcatenatedTidList;
import fr.liglab.lcm.internals.tidlist.TidList;
import fr.liglab.lcm.internals.transactions.ConcatenatedTransactionsList;
import fr.liglab.lcm.internals.transactions.TransactionsList;
import fr.liglab.lcm.internals.transactions.TransactionsWriter;
import gnu.trove.iterator.TIntIterator;

class Dataset {

	protected final TransactionsList transactions;

	protected boolean longTransactionMode = false;

	/**
	 * frequent item => array of occurrences indexes in "concatenated"
	 * Transactions are added in the same order in all occurrences-arrays.
	 */
	protected final TidList tidLists;

	protected Dataset(TransactionsList transactions) {
		this.transactions = transactions;
		this.tidLists = null;
	}

	Dataset(final Iterator<TransactionReader> transactions) {
		
		this.transactions = new ConcatenatedTransactionsList(true, counts.supportsSum, counts.transactionsCount);

		// prepare occurrences lists
		this.tidLists = new ConsecutiveItemsConcatenatedTidList(true, counts.supportCounts);

		// COPY
		TransactionsWriter writer = this.transactions.getWriter();
		while (transactions.hasNext()) {
			TransactionReader transaction = transactions.next();
			final int transId = writer.beginTransaction(transaction.getTransactionSupport());
			while (transaction.hasNext()) {
				final int item = transaction.next();
				writer.addItem(item);
				this.tidLists.addTransaction(item, transId);
			}
		}

		this.longTransactionMode = (counts.supportsSum / counts.transactionsCount) > LONG_TRANSACTION_MODE_THRESHOLD;
	}
	
	Dataset project(int extension, DatasetCountersRenamer extensionCounters) {
		double reductionRate = extensionCounters.transactionsCount / this.getConcatenatedTransactionCount();
		if (!this.longTransactionMode && reductionRate > FlexibleDatasetView.THRESHOLD) {
			extensionCounters.compactRebase(false, null);
			extensionCounters.reverseRenaming = this.counters.reverseRenaming;
			return new FlexibleDatasetView(extensionCounters, this, extension);
		} else {
			extensionCounters.compactRebase(true, this.counters.reverseRenaming);
			Iterator<TransactionReader> support = this.getSupport(extension);
			TransactionsFilteringDecorator filtered = new TransactionsFilteringDecorator(support,
					extensionCounters.supportCounts, extensionCounters.renaming);
			extensionCounters.renaming = null;
			return new FlexibleDataset(extensionCounters, filtered);
		}
	}

	protected double getConcatenatedTransactionCount() {
		return this.counters.transactionsCount;
	}

	@Override
	public Iterator<TransactionReader> getSupport(int item) {
		final TIntIterator it = this.tidLists.get(item);
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

	@Override
	public final int ppTest(int extension) {
		if (this.longTransactionMode) {
			return FastPrefixPreservingTest.ppTest(extension, this.counters, this.tidLists);
		} else {
			return -1;
		}
	}
}
