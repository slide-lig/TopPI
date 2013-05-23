package fr.liglab.lcm.internals.nomaps;

import java.util.Arrays;
import java.util.Calendar;

import fr.liglab.lcm.internals.FrequentsIterator;
import fr.liglab.lcm.internals.nomaps.Dataset.TransactionsIterable;
import fr.liglab.lcm.internals.nomaps.Selector.WrongFirstParentException;
import fr.liglab.lcm.io.FileReader;
import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * Represents an LCM recursion step. Its also acts as a Dataset factory.
 */
public final class ExplorationStep implements Cloneable {

	public static boolean verbose = false;
	public static boolean ultraVerbose = false;

	/**
	 * @see longTransactionsMode
	 */
	static final int LONG_TRANSACTION_MODE_THRESHOLD = 2000;

	/**
	 * When projecting on a item having a support count above
	 * VIEW_SUPPORT_THRESHOLD%, projection will be a DatasetView
	 */
	static final double VIEW_SUPPORT_THRESHOLD = 0.15;

	/**
	 * closure of parent's pattern UNION extension
	 */
	public final int[] pattern;

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

	protected final FrequentsIterator candidates;

	/**
	 * When an extension fails first-parent test, it ends up in this map. Keys
	 * are non-first-parent items associated to their actual first parent.
	 */
	private final TIntIntHashMap failedFPTests;

	/**
	 * When the average transaction length is above
	 * LONG_TRANSACTION_MODE_THRESHOLD, we prepend a FirstParentTest to selector
	 * chain and set this flag (for self and all substeps) FIXME could be more
	 * elegant with introspection ?
	 */
	private final boolean longTransactionsMode;

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
		this.longTransactionsMode = false;

		FileReader reader = new FileReader(path);
		this.counters = new Counters(minimumSupport, reader);
		reader.close(this.counters.renaming);

		this.pattern = this.counters.closure;

		this.dataset = new Dataset(this.counters, reader);

		this.candidates = this.counters.getExtensionsIterator();

		this.failedFPTests = new TIntIntHashMap();
	}

	private ExplorationStep(int[] pattern, int core_item, Dataset dataset, Counters counters, Selector selectChain,
			FrequentsIterator candidates, TIntIntHashMap failedFPTests, boolean longTransactionsMode) {
		super();
		this.pattern = pattern;
		this.core_item = core_item;
		this.dataset = dataset;
		this.counters = counters;
		this.selectChain = selectChain;
		this.candidates = candidates;
		this.failedFPTests = failedFPTests;
		this.longTransactionsMode = longTransactionsMode;
	}

	/**
	 * Finds an extension for current pattern in current dataset and returns the
	 * corresponding ExplorationStep (extensions are enumerated by ascending
	 * item IDs - in internal rebasing) Returns null when all valid extensions
	 * have been generated
	 */
	public ExplorationStep next() {
		if (this.candidates == null) {
			return null;
		}

		while (true) {
			int candidate = this.candidates.next();

			if (candidate < 0) {
				return null;
			}

			try {
				if (this.selectChain == null || this.selectChain.select(candidate, this)) {
					TransactionsIterable support = this.dataset.getSupport(candidate);

					// System.out.println("extending "+Arrays.toString(this.pattern)
					// + " with (base) "+candidate);
					// System.out.println(" with "+this.counters.getReverseRenaming()[candidate]);

					Counters candidateCounts = new Counters(this.counters.minSupport, support.iterator(), candidate,
							this.dataset.getIgnoredItems(), this.counters.maxFrequent);

					int greatest = Integer.MIN_VALUE;
					for (int i = 0; i < candidateCounts.closure.length; i++) {
						if (candidateCounts.closure[i] > greatest) {
							greatest = candidateCounts.closure[i];
						}
					}

					if (greatest > candidate) {
						throw new WrongFirstParentException(candidate, greatest);
					}

					// instanciateDataset may choose to compress renaming - if
					// not, at least it's set for now.
					candidateCounts.reuseRenaming(this.counters.reverseRenaming);

					return new ExplorationStep(this, candidate, candidateCounts, support);
				}
			} catch (WrongFirstParentException e) {
				addFailedFPTest(e.extension, e.firstParent);
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
	protected ExplorationStep(ExplorationStep parent, int extension, Counters candidateCounts,
			TransactionsIterable support) {

		this.core_item = extension;
		this.counters = candidateCounts;
		int[] reverseRenaming = parent.counters.reverseRenaming;

		if (verbose) {
			if (parent.pattern.length == 0 || ultraVerbose) {
				System.out.format("%1$tY/%1$tm/%1$td %1$tk:%1$tM:%1$tS - thread %2$d projecting %3$s with %4$s\n",
						Calendar.getInstance(), Thread.currentThread().getId(), Arrays.toString(parent.pattern),
						reverseRenaming[extension]);
			}
		}

		this.pattern = ItemsetsFactory
				.extendRename(candidateCounts.closure, extension, parent.pattern, reverseRenaming);

		if (this.counters.nbFrequents == 0 || this.counters.distinctTransactionsCount == 0) {
			this.candidates = null;
			this.failedFPTests = null;
			this.selectChain = null;
			this.dataset = null;
			this.longTransactionsMode = false;
		} else {
			this.failedFPTests = new TIntIntHashMap();

			if (parent.selectChain == null) {
				this.selectChain = null;
			} else {
				this.selectChain = parent.selectChain.copy();
			}

			// !\\ From here, order is important

			if (parent.longTransactionsMode) {
				this.longTransactionsMode = true;
			} else {
				final int averageLen = candidateCounts.distinctTransactionLengthSum
						/ candidateCounts.distinctTransactionsCount;
				this.longTransactionsMode = (averageLen) > LONG_TRANSACTION_MODE_THRESHOLD;

				if (this.longTransactionsMode) {
					this.selectChain = new FirstParentTest(this.selectChain);
				}
			}

			// indeed, instantiateDataset is influenced by longTransactionsMode

			this.dataset = instanciateDataset(parent, support);

			// and intanciateDataset may choose to trigger some renaming in
			// couters

			this.candidates = this.counters.getExtensionsIterator();

		}
	}

	/**
	 * @param candidateCounts
	 * @param extension
	 */
	private Dataset instanciateDataset(ExplorationStep parent, TransactionsIterable support) {
		double supportRate = this.counters.distinctTransactionsCount
				/ (double) parent.dataset.getStoredTransactionsCount();

		if (!this.longTransactionsMode && (supportRate) > VIEW_SUPPORT_THRESHOLD) {
			return new DatasetView(parent.dataset, this.counters, support, this.core_item);
		} else {
			int[] renaming = this.counters.compressRenaming(parent.counters.getReverseRenaming());

			TransactionsRenamingDecorator filtered = new TransactionsRenamingDecorator(support.iterator(), renaming);

			return new Dataset(this.counters, filtered);
		}
	}

	public synchronized int getFailedFPTest(final int item) {
		return this.failedFPTests.get(item);
	}

	private synchronized void addFailedFPTest(final int item, final int firstParent) {
		this.failedFPTests.put(item, firstParent);
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
		return new ExplorationStep(pattern, core_item, dataset.clone(), counters.clone(), selectChain, candidates,
				failedFPTests, longTransactionsMode);
	}
}
