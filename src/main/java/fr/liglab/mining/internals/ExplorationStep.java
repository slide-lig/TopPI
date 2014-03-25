package fr.liglab.mining.internals;

import fr.liglab.mining.CountersHandler;
import fr.liglab.mining.CountersHandler.TopLCMCounters;
import fr.liglab.mining.internals.Dataset.TransactionsIterable;
import fr.liglab.mining.internals.Selector.WrongFirstParentException;
import fr.liglab.mining.io.FileFilteredReader;
import fr.liglab.mining.io.FileReader;
import fr.liglab.mining.io.PerItemTopKCollector;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;

/**
 * Represents an LCM recursion step. Its also acts as a Dataset factory.
 */
public final class ExplorationStep implements Cloneable {

	public static boolean verbose = false;
	public static boolean ultraVerbose = false;
	public static boolean LOG_EPSILONS = false;

	public final static String KEY_VIEW_SUPPORT_THRESHOLD = "toplcm.threshold.view";
	public final static String KEY_LONG_TRANSACTIONS_THRESHOLD = "toplcm.threshold.long";

	/**
	 * @see longTransactionsMode
	 */
	static int LONG_TRANSACTION_MODE_THRESHOLD = Integer.parseInt(System.getProperty(KEY_LONG_TRANSACTIONS_THRESHOLD,
			"2000"));

	/**
	 * When projecting on a item having a support count above
	 * VIEW_SUPPORT_THRESHOLD%, projection will be a DatasetView
	 */
	static double VIEW_SUPPORT_THRESHOLD = Double.parseDouble(System.getProperty(KEY_VIEW_SUPPORT_THRESHOLD, "0.15"));

	/**
	 * When set to true we stick to a complete LCMv2 implementation, with
	 * predictive prefix-preservation tests and compressions at all steps.
	 * Setting this to false is better when mining top-k-per-item patterns.
	 */
	public static boolean LCM_STYLE = false;

	private static final ExplorationStep fake = new ExplorationStep();

	public static boolean COMPRESS_LVL1 = false;

	/**
	 * Extension item that led to this recursion step. Already included in
	 * "pattern".
	 */
	public final int core_item;

	public final Dataset dataset;

	public final Counters counters;

	/**
	 * Selectors chain - may be null when empty
	 */
	protected Selector selectChain;

	public final FrequentsIterator candidates;

	/**
	 * When an extension fails first-parent test, it ends up in this map. Keys
	 * are non-first-parent items associated to their actual first parent.
	 */
	private final TIntIntHashMap failedFPTests;

	// only used to make a fake one
	private ExplorationStep() {
		this.failedFPTests = null;
		this.dataset = null;
		this.counters = null;
		this.core_item = Integer.MIN_VALUE;
		this.candidates = null;
	}

	/**
	 * Start exploration on a dataset contained in a file.
	 * 
	 * @param minimumSupport
	 * @param path
	 *            to an input file in ASCII format. Each line should be a
	 *            transaction containing space-separated item IDs.
	 */
	public ExplorationStep(int minimumSupport, String path) {
		this.core_item = Integer.MAX_VALUE;
		this.selectChain = null;

		FileReader reader = new FileReader(path);
		this.counters = new Counters(minimumSupport, reader);
		reader.close(this.counters.renaming);

		this.dataset = new Dataset(this.counters, reader);

		this.candidates = this.counters.getExtensionsIterator();

		this.failedFPTests = new TIntIntHashMap();
	}

	public ExplorationStep(int minimumSupport, FileFilteredReader reader, int maxItem, int[] reverseGlobalRenaming) {
		this.core_item = Integer.MAX_VALUE;
		this.selectChain = null;

		this.counters = new Counters(minimumSupport, reader, maxItem + 1, null, maxItem + 1, reverseGlobalRenaming,
				new int[] {});
		int[] renaming = this.counters.compressRenaming(reverseGlobalRenaming);
		reader.close(renaming);

		this.dataset = new Dataset(this.counters, reader);

		if (this.counters.pattern.length > 0) {
			for (int i = 0; i < this.counters.pattern.length; i++) {
				this.counters.pattern[i] = reverseGlobalRenaming[this.counters.pattern[i]];
			}
		}

		this.candidates = this.counters.getExtensionsIterator();
		this.failedFPTests = new TIntIntHashMap();
	}

	/**
	 * Start exploration on one of Hadoop's sub-dataset
	 * 
	 * @param minimumSupport
	 * @param transactions
	 * @param maxItem
	 * @param reverseRenaming
	 */
	public ExplorationStep(int minimumSupport, Iterable<TransactionReader> transactions, int maxItem,
			int[] reverseRenaming) {
		this.core_item = Integer.MAX_VALUE;
		this.selectChain = null;

		this.counters = new Counters(minimumSupport, transactions.iterator(), maxItem + 1, null, maxItem + 1,
				reverseRenaming, new int[] {});
		Iterator<TransactionReader> trans = transactions.iterator();

		int[] renaming = this.counters.compressRenaming(reverseRenaming);
		trans = new TransactionsRenamingDecorator(trans, renaming);

		this.dataset = new Dataset(this.counters, trans);
		// FIXME
		// from here we actually instantiated 3 times the dataset's size
		// once in dataset.transactions, one in dataset.tidLists (both are OK)
		// and
		// worse, once again in transactions.cached

		if (this.counters.pattern.length > 0) {
			for (int i = 0; i < this.counters.pattern.length; i++) {
				this.counters.pattern[i] = reverseRenaming[this.counters.pattern[i]];
			}
		}

		this.candidates = this.counters.getExtensionsIterator();
		this.failedFPTests = new TIntIntHashMap();
	}

	private ExplorationStep(int core_item, Dataset dataset, Counters counters, Selector selectChain,
			FrequentsIterator candidates, TIntIntHashMap failedFPTests) {
		super();
		this.core_item = core_item;
		this.dataset = dataset;
		this.counters = counters;
		this.selectChain = selectChain;
		this.candidates = candidates;
		this.failedFPTests = failedFPTests;
	}

	/**
	 * Finds an extension for current pattern in current dataset and returns the
	 * corresponding ExplorationStep (extensions are enumerated by ascending
	 * item IDs - in internal rebasing) Returns null when all valid extensions
	 * have been generated If it has not been done before, this method will
	 * perform the preliminary breadth-first exploration
	 */
	public ExplorationStep next(PerItemTopKCollector collector) {
		if (this.candidates == null) {
			return null;
		}

		while (true) {
			ExplorationStep res;

			int candidate = this.candidates.next();

			if (candidate < 0) {
				return null;
			} else {
				res = this.doDepthExplorationFromScratch(candidate, collector);
				if (LOG_EPSILONS) {
					synchronized(System.out) {
						if (res.counters != null && this.counters !=null && this.counters.pattern != null && this.counters.pattern.length == 0) {
							System.out.println(candidate+" "+res.counters.minSupport);
						}
					}
				}
			}

			if (res != fake) {
				return res;
			}
		}
	}

	/**
	 * for a normal LCM use you should use .next() instead
	 * 
	 * @param item
	 *            internal ID
	 * @return the projected step, or null in case of failed first-parent test
	 */
	public ExplorationStep project(int candidate, int k) {
		TransactionsIterable support = this.dataset.getSupport(candidate);

		Counters candidateCounts = new Counters(this.counters.minSupport, support.iterator(),
				this.counters.maxFrequent + 1, null, this.counters.maxFrequent + 1, this.counters.getReverseRenaming(),
				new int[] {});
		// Counters candidateCounts = new Counters(this.counters.minSupport,
		// support.iterator());

		int[] renaming = candidateCounts.compressRenaming(this.counters.getReverseRenaming(), k);

		TransactionsRenamingDecorator filtered = new TransactionsRenamingDecorator(support.iterator(), renaming);
		
		if (verbose) {
			System.err.format("{\"time\":\"%1$tY/%1$tm/%1$td %1$tk:%1$tM:%1$tS\",\"thread\":%2$d,\"projectOn\":%3$d,\"originalId\":%4$d}\n",
							Calendar.getInstance(), Thread.currentThread().getId(), candidate, 
							this.counters.getReverseRenaming()[candidate]);
		}
		
		return new ExplorationStep(candidateCounts,filtered);
	}

	private ExplorationStep(Counters candidateCounts, Iterator<TransactionReader> support) {
		this.failedFPTests = new TIntIntHashMap();
		this.counters = candidateCounts;
		this.core_item = Integer.MAX_VALUE;
		this.candidates = this.counters.getExtensionsIterator();
		this.dataset = new Dataset(candidateCounts, support);
		this.selectChain = null;
	}

	/**
	 * Instantiate state for a valid extension.
	 * 
	 * @param parent
	 * @param extension
	 *            a first-parent extension from parent step
	 * @param candidateCounts
	 *            extension's counters from parent step
	 * @param support
	 *            previously-computed extension's support
	 */
	@SuppressWarnings("boxing")
	protected ExplorationStep(ExplorationStep parent, int extension, Counters candidateCounts,
			TransactionsIterable support) {

		this.core_item = extension;
		this.counters = candidateCounts;
		int[] reverseRenaming = parent.counters.reverseRenaming;

		if (verbose) {
			if (parent.counters.pattern.length == 0 || ultraVerbose) {
				System.err
						.format("{\"time\":\"%1$tY/%1$tm/%1$td %1$tk:%1$tM:%1$tS\",\"thread\":%2$d,\"pattern\":%3$s,\"extension_internal\":%4$d,\"extension\":%5$d}\n",
								Calendar.getInstance(), Thread.currentThread().getId(),
								Arrays.toString(parent.counters.pattern), extension, reverseRenaming[extension]);
			}
		}

		if (this.counters.nbFrequents == 0 || this.counters.distinctTransactionsCount == 0) {
			this.candidates = null;
			this.failedFPTests = null;
			this.selectChain = null;
			this.dataset = null;
		} else {
			this.failedFPTests = new TIntIntHashMap();
			this.dataset = instanciateDatasetAndPickSelectors(parent, support);
			this.candidates = this.counters.getExtensionsIterator();
		}
	}

	private Dataset instanciateDatasetAndPickSelectors(ExplorationStep parent, TransactionsIterable support) {
		final double supportRate = this.counters.distinctTransactionsCount
				/ (double) parent.dataset.getStoredTransactionsCount();

		final int averageLen = this.counters.distinctTransactionLengthSum / this.counters.distinctTransactionsCount;

		if (averageLen < LONG_TRANSACTION_MODE_THRESHOLD && supportRate > VIEW_SUPPORT_THRESHOLD) {
			copySelectChainWithoutFPT(parent.selectChain);
			return new DatasetView(parent.dataset, this.counters, support, this.core_item);
		} else {
			if (averageLen > LONG_TRANSACTION_MODE_THRESHOLD) {
				copySelectChainWithFPT(parent.selectChain);
			} else {
				copySelectChainWithoutFPT(parent.selectChain);
			}

			final int[] renaming;
			boolean compress = COMPRESS_LVL1 && parent.core_item == Integer.MAX_VALUE
					&& this.core_item < Integer.MAX_VALUE;

			if (compress) {
				renaming = this.counters.compressRenaming(null);
			} else {
				renaming = this.counters.compressRenaming(null, Integer.MAX_VALUE);
			}
			TransactionsRenamingDecorator filtered = new TransactionsRenamingDecorator(support.iterator(), renaming);

			// FIXME the last argument is now obsolete
			Dataset dataset = new Dataset(this.counters, filtered, Integer.MAX_VALUE);

			if (compress) {
				// FIXME FIXME core_item refers an UNCOMPRESSED id
				dataset.compress(this.core_item);
			}

			return dataset;
		}
	}

	private void copySelectChainWithFPT(Selector chain) {
		if (chain == null) {
			this.selectChain = FirstParentTest.getTailInstance();
		} else if (chain.contains(FirstParentTest.class)) {
			this.selectChain = chain.copy();
		} else {
			this.selectChain = chain.append(FirstParentTest.getTailInstance());
		}
	}

	private void copySelectChainWithoutFPT(Selector chain) {
		if (chain == null) {
			this.selectChain = null;
		} else if (chain.contains(FirstParentTest.class)) {
			// /// FIXME: this is not really correct
			// /// that selectChain stuff is not adapated
			this.selectChain = chain.copy(null);
		} else {
			this.selectChain = chain.copy();
		}
	}

	public int getFailedFPTest(final int item) {
		synchronized (this.failedFPTests) {
			return this.failedFPTests.get(item);
		}
	}

	private void addFailedFPTest(final int item, final int firstParent) {
		synchronized (this.failedFPTests) {
			this.failedFPTests.put(item, firstParent);
		}

		CountersHandler.increment(TopLCMCounters.FailedFPTests);
	}

	public void appendSelector(Selector s) {
		if (this.selectChain == null) {
			this.selectChain = s;
		} else {
			this.selectChain = this.selectChain.append(s);
		}
	}

	public int getCatchedWrongFirstParentCount() {
		if (this.failedFPTests == null) {
			return 0;
		} else {
			return this.failedFPTests.size();
		}
	}

	public ExplorationStep copy() {
		return new ExplorationStep(core_item, dataset.clone(), counters.clone(), selectChain, candidates, failedFPTests);
	}
	
	protected ExplorationStep doDepthExplorationFromScratch(int candidate, PerItemTopKCollector collector) {
		// if (this.counters.pattern.length == 1 && this.counters.pattern[0] ==
		// 338 && this.counters.reverseRenaming[candidate] ==497) {
		// System.err.println("extending local " + candidate + " " +
		// this.counters.reverseRenaming[candidate]);
		// }
		try {
			if (selectChain.select(candidate, ExplorationStep.this)) {
				TransactionsIterable support = dataset.getSupport(candidate);

				Counters candidateCounts = new Counters(counters.minSupport, support.iterator(), candidate,
						dataset.getIgnoredItems(), counters.maxFrequent, counters.reverseRenaming, counters.pattern);

				int greatest = Integer.MIN_VALUE;
				for (int i = 0; i < candidateCounts.closure.length; i++) {
					if (candidateCounts.closure[i] > greatest) {
						greatest = candidateCounts.closure[i];
					}
				}

				if (greatest > candidate) {
					collector.collect(candidateCounts.transactionsCount, candidateCounts.pattern);
					throw new WrongFirstParentException(candidate, greatest);
				}
				collector.collect(candidateCounts.transactionsCount, candidateCounts.pattern);
				candidateCounts.raiseMinimumSupport(collector);
				ExplorationStep next = new ExplorationStep(ExplorationStep.this, candidate, candidateCounts, support);

				return next;
			}
		} catch (WrongFirstParentException e) {
			addFailedFPTest(e.extension, e.firstParent);
		}
		return fake;
	}
}
