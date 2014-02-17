package fr.liglab.mining.internals;

import fr.liglab.mining.TopLCM;
import fr.liglab.mining.TopLCM.TopLCMCounters;
import fr.liglab.mining.internals.Dataset.TransactionsIterable;
import fr.liglab.mining.internals.Selector.WrongFirstParentException;
import fr.liglab.mining.io.FileFilteredReader;
import fr.liglab.mining.io.FileReader;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.io.PerItemTopKCollector.PatternWithFreq;
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

	public final static String KEY_VIEW_SUPPORT_THRESHOLD = "toplcm.threshold.view";
	public final static String KEY_LONG_TRANSACTIONS_THRESHOLD = "toplcm.threshold.long";
	public final static String KEY_BREADTH_SIZE = "toplcm.threshold.breadth";

	static int BREADTH_SIZE = Integer.parseInt(System.getProperty(KEY_BREADTH_SIZE, "1"));

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

	protected final MiningTasksIterator candidates;

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

		this.candidates = this.counters.getExtensionsIterator(BREADTH_SIZE);

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

		this.candidates = this.counters.getExtensionsIterator(BREADTH_SIZE);
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

		this.candidates = this.counters.getExtensionsIterator(BREADTH_SIZE);
		this.failedFPTests = new TIntIntHashMap();
	}

	private ExplorationStep(int core_item, Dataset dataset, Counters counters, Selector selectChain,
			MiningTasksIterator candidates, TIntIntHashMap failedFPTests) {
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
		while (true) {
			if (this.candidates == null) {
				return null;
			}
			MiningTask task = this.candidates.next(this);
			if (task != null) {
				ExplorationStep res = task.execute(collector);
				if (res != fake) {
					return res;
				}
			} else {
				return null;
			}
		}
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
			this.candidates = this.counters.getExtensionsIterator(BREADTH_SIZE);
		}
	}

	private Dataset instanciateDatasetAndPickSelectors(ExplorationStep parent, TransactionsIterable support) {
		final double supportRate = this.counters.distinctTransactionsCount
				/ (double) parent.dataset.getStoredTransactionsCount();

		final int averageLen = this.counters.distinctTransactionLengthSum / this.counters.distinctTransactionsCount;

		if (averageLen < LONG_TRANSACTION_MODE_THRESHOLD && supportRate > VIEW_SUPPORT_THRESHOLD) {
			if (parent.dataset instanceof DatasetView) {
				this.selectChain = parent.selectChain.copy();
			} else {
				this.selectChain = parent.selectChain.copy(null);
			}

			return new DatasetView(parent.dataset, this.counters, support, this.core_item);
		} else {
			if (parent.dataset instanceof DatasetView) {
				this.selectChain = parent.selectChain.copy(FirstParentTest.getTailInstance());
			} else {
				this.selectChain = parent.selectChain.copy();
			}

			final int[] renaming = this.counters.compressRenaming(null);
			TransactionsRenamingDecorator filtered = new TransactionsRenamingDecorator(support.iterator(), renaming);

			try {
				Dataset dataset = new Dataset(this.counters, filtered, Integer.MAX_VALUE); // TODO
																							// the
																							// last
																							// argument
																							// is
																							// now
																							// obsolete
				dataset.compress(this.core_item); // FIXME FIXME core_item
													// refers an UNCOMPRESSED id

				return dataset;
			} catch (ArrayIndexOutOfBoundsException e) {
				System.out.println("WAT core_item = " + this.core_item);
				e.printStackTrace();
				System.exit(1);
			}

			return null;
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
		((TopLCM.TopLCMThread) Thread.currentThread()).counters[TopLCMCounters.FailedFPTests.ordinal()]++;
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

	public Progress getProgression() {
		return new Progress();
	}

	public final class Progress {
		public final int current;
		public final int last;

		protected Progress() {
			this.current = candidates.peek();
			this.last = candidates.last();
		}
	}

	protected interface MiningTask {
		ExplorationStep execute(PerItemTopKCollector collector);

	}

	protected class BreadthExploration implements MiningTask {
		private int candidate;
		private final MiningTasksIterator iterator;

		public BreadthExploration(int candidate, MiningTasksIterator iterator) {
			super();
			this.candidate = candidate;
			this.iterator = iterator;
		}

		@Override
		public ExplorationStep execute(PerItemTopKCollector collector) {
			try {
				if (selectChain.select(candidate, ExplorationStep.this)) {
					if (dataset instanceof DatasetView) {
						TransactionsIterable support = dataset.getSupport(candidate);

						Counters candidateCounts = new Counters(counters.minSupport, support.iterator(), candidate,
								dataset.getIgnoredItems(), counters.maxFrequent, counters.reverseRenaming,
								counters.pattern);

						int greatest = Integer.MIN_VALUE;
						for (int i = 0; i < candidateCounts.closure.length; i++) {
							if (candidateCounts.closure[i] > greatest) {
								greatest = candidateCounts.closure[i];
							}
						}

						if (greatest > candidate) {
							throw new WrongFirstParentException(candidate, greatest);
						}

						PatternWithFreq tmp = collector.preCollect(candidateCounts.transactionsCount, candidate,
								candidateCounts);
						tmp.keepForLater(candidateCounts, support);
						iterator.pushFutureWork(tmp);
					} else {
						int support = counters.supportCounts[candidate];
						PatternWithFreq tmp = collector.preCollect(support, counters.pattern, candidate,
								counters.reverseRenaming[candidate]);
						iterator.pushFutureWork(tmp);
					}
				}
			} catch (WrongFirstParentException e) {
				addFailedFPTest(e.extension, e.firstParent);
			}
			return fake;
		}
	}

	protected class DepthExplorationPrepared implements MiningTask {
		private final PatternWithFreq holder;

		public DepthExplorationPrepared(PatternWithFreq holder) {
			super();
			this.holder = holder;
		}

		// handle still in topk before
		@Override
		public ExplorationStep execute(PerItemTopKCollector collector) {
			if (holder.isStillInTopKs()) {
				final int candidate = holder.extension;
				Counters candidateCounts = holder.getMemoizedCounters();
				TransactionsIterable support = holder.getMemoizedSupport();

				if (support == null) {
					support = dataset.getSupport(candidate);

					candidateCounts = new Counters(counters.minSupport, support.iterator(), candidate,
							dataset.getIgnoredItems(), counters.maxFrequent, counters.reverseRenaming, counters.pattern);
				}

				ExplorationStep next = new ExplorationStep(ExplorationStep.this, candidate, candidateCounts, support);

				// now let's collect the right pattern
				holder.setPattern(next.counters.pattern);
				int insertionsCounter = 0;
				for (int item : candidateCounts.closure) {
					if (collector.insertPatternInTop(holder, counters.reverseRenaming[item])) {
						insertionsCounter++;
					}
				}
				if (insertionsCounter > 0) {
					holder.incrementRefCount(insertionsCounter);
				}

				return next;
			} else {
				return fake;
			}
		}
	}

	protected class DepthExplorationFromScratch implements MiningTask {

		private int candidate;

		public DepthExplorationFromScratch(int candidate) {
			this.candidate = candidate;
		}

		@Override
		public ExplorationStep execute(PerItemTopKCollector collector) {
			try {
				if (selectChain.select(candidate, ExplorationStep.this)) {
					TransactionsIterable support = dataset.getSupport(candidate);

					Counters candidateCounts = new Counters(counters.minSupport, support.iterator(), candidate,
							dataset.getIgnoredItems(), counters.maxFrequent, counters.reverseRenaming, counters.pattern);

					ExplorationStep next = new ExplorationStep(ExplorationStep.this, candidate, candidateCounts,
							support);

					collector.collect(candidateCounts.transactionsCount, next.counters.pattern);

					return next;
				}
			} catch (WrongFirstParentException e) {
				addFailedFPTest(e.extension, e.firstParent);
			}
			return fake;
		}
	}
}
