package fr.liglab.mining.internals;

import java.util.Arrays;
import java.util.Calendar;

import fr.liglab.mining.TopLCM;
import fr.liglab.mining.TopLCM.TopLCMCounters;
import fr.liglab.mining.internals.Dataset.TransactionsIterable;
import fr.liglab.mining.internals.Selector.WrongFirstParentException;
import fr.liglab.mining.io.FileReader;
import fr.liglab.mining.util.ItemsetsFactory;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * Represents an LCM recursion step. Its also acts as a Dataset factory.
 */
public final class ExplorationStep implements Cloneable {

	public static boolean verbose = false;
	public static boolean ultraVerbose = false;
	
	public final static String KEY_VIEW_SUPPORT_THRESHOLD = "toplcm.threshold.view";
	public final static String KEY_LONG_TRANSACTIONS_THRESHOLD = "toplcm.threshold.long";
	
	/**
	 * @see longTransactionsMode
	 */
	static int LONG_TRANSACTION_MODE_THRESHOLD = Integer.parseInt(
			System.getProperty(KEY_LONG_TRANSACTIONS_THRESHOLD, "2000"));

	/**
	 * When projecting on a item having a support count above
	 * VIEW_SUPPORT_THRESHOLD%, projection will be a DatasetView
	 */
	static double VIEW_SUPPORT_THRESHOLD = Double.parseDouble(
			System.getProperty(KEY_VIEW_SUPPORT_THRESHOLD, "0.15"));

	/**
	 * When set to true we stick to a complete LCMv2 implementation, with predictive 
	 * prefix-preservation tests and compressions at all steps.
	 * Setting this to false is better when mining top-k-per-item patterns.
	 */
	public static boolean LCM_STYLE = true;

	/**
	 * closure of parent's pattern UNION extension
	 */
	public final int[] pattern;

	/**
	 * Extension item that led to this recursion step. Already included in
	 * "pattern".
	 */
	public final int core_item;
	
	/**
	 * Don't EVER access the dataset field directly (even internally)
	 * @see getDataset
	 */
	protected Dataset dataset = null;
	
	/**
	 * A to-be-filtered dataset
	 * @see getDataset
	 */
	protected TransactionsIterable lazyDataset = null;

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

	private final boolean predictiveFPTestMode;

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
		this.predictiveFPTestMode = false;

		FileReader reader = new FileReader(path);
		this.counters = new Counters(minimumSupport, reader);
		reader.close(this.counters.renaming);

		this.pattern = this.counters.closure;

		this.dataset = new Dataset(this.counters, reader);

		this.candidates = this.counters.getExtensionsIterator();

		this.failedFPTests = new TIntIntHashMap();
	}

	private ExplorationStep(int[] pattern, int core_item, Dataset dataset, Counters counters, Selector selectChain,
			FrequentsIterator candidates, TIntIntHashMap failedFPTests, boolean predictiveFPTestMode) {
		super();
		this.pattern = pattern;
		this.core_item = core_item;
		this.dataset = dataset;
		this.counters = counters;
		this.selectChain = selectChain;
		this.candidates = candidates;
		this.failedFPTests = failedFPTests;
		this.predictiveFPTestMode = predictiveFPTestMode;
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
				if (this.dataset == null && this.lazyDataset != null) {
					((TopLCM.TopLCMThread) Thread.currentThread()).counters[TopLCMCounters.AvoidedFilterings.ordinal()]++;
				}
				
				return null;
			}

			try {
				if (this.selectChain == null || this.selectChain.select(candidate, this)) {
					boolean toBeRefreshed = (this.dataset == null);
					Dataset dataset = this.getDataset();
					
					if (toBeRefreshed) {
						candidate = this.candidates.next();
					}
					
					TransactionsIterable support = dataset.getSupport(candidate);

					// System.out.println("extending "+Arrays.toString(this.pattern)+
					// " with "+
					// candidate+" ("+this.counters.getReverseRenaming()[candidate]+")");

					Counters candidateCounts = new Counters(this.counters.minSupport, support.iterator(), candidate,
							dataset.getIgnoredItems(), this.counters.maxFrequent);

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
				System.err.format("%1$tY/%1$tm/%1$td %1$tk:%1$tM:%1$tS - thread %2$d projecting %3$s with %4$s\n",
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
			this.predictiveFPTestMode = false;
		} else {
			this.failedFPTests = new TIntIntHashMap();

			if (parent.selectChain == null) {
				this.selectChain = null;
			} else {
				this.selectChain = parent.selectChain.copy();
			}

			// ! \\ From here, order is important ...
			
			if (parent.predictiveFPTestMode) {
				this.predictiveFPTestMode = true;
			} else {
				final int averageLen = candidateCounts.distinctTransactionLengthSum
						/ candidateCounts.distinctTransactionsCount;

				this.predictiveFPTestMode = LCM_STYLE || averageLen > LONG_TRANSACTION_MODE_THRESHOLD;
				if (this.predictiveFPTestMode) {
					this.selectChain = new FirstParentTest(this.selectChain);
				}
			}

			// ... because instantiateDataset is influenced by longTransactionsMode
			this.instanciateDataset(parent, support);

			// and intanciateDataset may choose to trigger some renaming in counters
			this.candidates = this.counters.getExtensionsIterator();
		}
	}
	
	private void instanciateDataset(final ExplorationStep parent, final TransactionsIterable support) {
		final double supportRate = this.counters.distinctTransactionsCount
				/ (double) parent.dataset.getStoredTransactionsCount();

		if (!this.predictiveFPTestMode && (supportRate) > VIEW_SUPPORT_THRESHOLD) {
			this.dataset = new DatasetView(parent.dataset, this.counters, support, this.core_item);
		} else {
			this.lazyDataset = support;
		}
	}
	
	/**
	 * WARNING : lazy instanciation may happen and cause an item 
	 * @return the projected dataset 
	 */
	public Dataset getDataset() {
		if (this.dataset == null && this.lazyDataset != null) {
			this.dataset = instanciateLazyDatasetAndUpdateCandidatesIterator(this);
			this.lazyDataset = null;
		}
		
		return this.dataset;
	}
	
	/**
	 * We use this method to trigger dataset filtering and renaming compression only when needed, ie. when we found a valid 
	 * candidate extension in the current ExplorationStep (it may not happen, for instance when k=1)
	 * 
	 * This method will alter target's candidates iterator, this is why an ExplorationStep becomes 
	 * thread-safe (stealable) only once its dataset instanciation is done.
	 */
	private static Dataset instanciateLazyDatasetAndUpdateCandidatesIterator(final ExplorationStep target) {
		final int[] renaming = target.counters.compressRenamingAndUpdateCandidatesIterator(target.candidates);
		TransactionsRenamingDecorator filtered = new TransactionsRenamingDecorator(target.lazyDataset.iterator(), renaming);
		
		final int tidsLimit = target.predictiveFPTestMode ? Integer.MAX_VALUE : target.counters.getMaxCandidate()+1;
		Dataset dataset = new Dataset(target.counters, filtered, tidsLimit);
		
		if (LCM_STYLE) {
			dataset.compress(target.core_item);
		}
		return dataset;
	}
	
	/**
	 * @return true if another thread may steal this job
	 */
	public boolean isStealable(){
		return this.lazyDataset == null;
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
				failedFPTests, predictiveFPTestMode);
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
}
