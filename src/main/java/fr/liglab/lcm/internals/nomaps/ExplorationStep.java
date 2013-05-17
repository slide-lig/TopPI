package fr.liglab.lcm.internals.nomaps;

import java.util.Iterator;

import fr.liglab.lcm.internals.FrequentsIterator;
import fr.liglab.lcm.internals.TransactionReader;
import fr.liglab.lcm.internals.nomaps.Selector.WrongFirstParentException;
import fr.liglab.lcm.io.FileReader;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * Represents an LCM recursion step. Its also acts as a Dataset factory.
 */
public final class ExplorationStep {

	/**
	 * if we construct on transactions having an average length above this value :
	 * - fast-prefix-preserving test will be done
	 * - it will never project to a ConcatenatedDatasetView
	 */
	static final int LONG_TRANSACTION_MODE_THRESHOLD = 2000;
	
	/**
	 * When projecting on a item having a support count above VIEW_SUPPORT_THRESHOLD%, 
	 * projection will be a DatasetView
	 */
	static final double VIEW_SUPPORT_THRESHOLD = 0.15;
	
	/**
	 * closure of parent's pattern UNION extension
	 */
	public final int[] pattern;
	
	/**
	 * Extension item that led to this recursion step. Already included in "pattern".
	 */
	final int core_item;
	
	public final Dataset dataset;
	
	public final Counters counters;
	
	/**
	 * Selectors chain - may be null when empty
	 */
	protected Selector selectChain;
	
	protected final FrequentsIterator candidates;
	
	/**
	 * When an extension fails first-parent test, it ends up in this map.
	 * Keys are non-first-parent items associated to their actual first parent.
	 */
	final TIntIntHashMap failedFPTests;
	
	/**
	 * Start exploration on a dataset contained in a file.
	 * @param minimumSupport
	 * @param path to an input file in ASCII format. Each line should be a transaction containing space-separated item IDs.
	 */
	public ExplorationStep(int minimumSupport, String path) {
		this.core_item = Integer.MAX_VALUE;
		this.selectChain = null;
		
		FileReader reader = new FileReader(path);
		this.counters = new Counters(minimumSupport, reader);
		reader.close();
		
		this.pattern = this.counters.closure;
		
		reader = new FileReader(path);
		Iterator<TransactionReader> transactions = new TransactionsRenamerFilterSorter(reader, this.counters.renaming);
		this.dataset = new Dataset(this.counters, transactions);
		reader.close();
		
		this.candidates = this.counters.getFrequentsIterator(this.core_item);
		
		this.failedFPTests = new TIntIntHashMap();
	}
	
	
	/**
	 * Finds an extension for current pattern in current dataset and returns the corresponding 
	 * ExplorationStep (extensions are enumerated by ascending item IDs - in internal rebasing) 
	 * Returns null when all valid extensions have been generated
	 */
	public ExplorationStep next() {
		while(true) {
			int candidate = this.candidates.next();
			
			if (candidate < 0) {
				return null;
			} else {
				try {
					if (this.selectChain == null || this.selectChain.select(candidate, this)) {
						
						Iterator<TransactionReader> support = this.dataset.getSupport(candidate);
						
						new Counters(this.counters.minSupport, support, candidate, 
								this.dataset.getIgnoredItems(), this.counters.maxFrequent);
						
						// TODO 
						// check discovered closure
						// new ExplorationStep(this, candidate, counters) --> will build dataset, handle rebasing and put pattern back to original names
						
					}
				} catch (WrongFirstParentException e) {
					this.failedFPTests.put(e.extension, e.firstParent);
				}
			}
		}
	}
}
