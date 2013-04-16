package fr.liglab.lcm;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.ExtensionsIterator;
import fr.liglab.lcm.io.PatternsCollector;
import fr.liglab.lcm.util.HeapDumper;
import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;

/**
 * LCM implementation, based on UnoAUA04 :
 * "An Efficient Algorithm for Enumerating Closed Patterns in Transaction Databases"
 * by Takeaki Uno el. al.
 * 
 * Turn on starters' logging by setting 
 * log4j.logger.fr.liglab.lcm.LCM=DEBUG
 * in log4j.properties (or use -Dlog4j.configuration=PATH_TO_FILE)
 */
public class LCM {
	private final Log logger;
	private final PatternsCollector collector;

	private int explored = 0;
	private int explorecut = 0;
	private int pptestcut = 0;

	public LCM(PatternsCollector patternsCollector) {
		collector = patternsCollector;
		logger = LogFactory.getLog(getClass());
	}

	/**
	 * Initial invocation
	 */
	public void lcm(final Dataset dataset) {
		// usually, it's empty
		int[] pattern = dataset.getDiscoveredClosureItems();

		if (pattern.length > 0) {
			collector.collect(dataset.getTransactionsCount(), pattern);
		}

		extensionLoop(pattern, dataset, true);
	}

	/**
	 * This function will output clo(pattern U {extension}), and (in recursive
	 * calls) all closed frequent closed itemsets prefixed by
	 * "pattern U {extension}"
	 * 
	 * HEAVY ASSUMPTIONS :
	 * 
	 * @param pattern
	 *            is a closed frequent itemset, freshly found out of :
	 * @param parent_dataset
	 *            - pattern's support
	 * @param extension
	 *            is an item known to yield a prefix-preserving extension of P
	 * @throws DontExploreThisBranchException
	 */
	public void lcm(final int[] pattern, final Dataset parent_dataset,
			int extension) throws DontExploreThisBranchException {

		try {
			final Dataset dataset = parent_dataset.getProjection(extension);

			int[] Q = ItemsetsFactory.extend(dataset.getDiscoveredClosureItems(), extension, pattern);

			collector.collect(dataset.getTransactionsCount(), Q);

			extensionLoop(Q, dataset, false);
		} catch (OutOfMemoryError e) {
			if (HeapDumper.basePath != null) {
				HeapDumper.dumpThemAll();
			}
			throw new RuntimeException(
					"OutOfMemoryError while extending pattern "
							+ Arrays.toString(pattern) + " with " + extension);
		}
	}

	private void extensionLoop(final int[] pattern, final Dataset dataset, final boolean log) {
		ExtensionsIterator iterator = dataset.getCandidatesIterator();
		int[] sortedFreqs = iterator.getSortedFrequents();
		TIntIntMap supportCounts = dataset.getSupportCounts();

		TIntIntMap failedPPTests = new TIntIntHashMap();
		int candidate;
		int previousExplore = -1;
		int previousCandidate = -1;
		
		while (true) {
			try {
				candidate = iterator.getExtension();
				
				int explore = collector.explore(pattern, candidate, sortedFreqs,
						supportCounts, failedPPTests, previousCandidate,
						previousExplore);
				if (explore < 0) {
					try {
						if (log && this.logger.isDebugEnabled()) {
							this.logger.debug("Extending "+Arrays.toString(pattern)+" with "+candidate);
						}
						lcm(pattern, dataset, candidate);
						explored++;
					} catch (DontExploreThisBranchException e) {
						failedPPTests.put(candidate, e.firstParent);
						pptestcut++;
					}

				} else {
					previousExplore = explore;
					previousCandidate = candidate;
					explorecut++;
				}
			} catch (DontExploreThisBranchException exn) {
				pptestcut++;
			}
		}
	}

	/**
	 * This exception must be thrown by methods invoked directly or not by lcm()
	 * when they find out that their search branch is uninteresting
	 */
	public static class DontExploreThisBranchException extends Exception {
		private static final long serialVersionUID = 2969583589161047791L;

		public final int firstParent;
		public final int extension;

		/**
		 * @param extension the tested extension
		 * @param foundFirstParent
		 *            a item found in closure > extension
		 */
		public DontExploreThisBranchException(int exploredExtension, int foundFirstParent) {
			this.firstParent = foundFirstParent;
			this.extension = exploredExtension;
		}
	}

	public String toString() {
		return "LCM exploration : " + explored + " patterns explored / "
				+ explorecut + " aborted by explore() / " + pptestcut
				+ " aborted by ppTest";
	}

	public int getExplorecut() {
		return explorecut;
	}

	public int getExplored() {
		return explored;
	}

	public int getPptestcut() {
		return pptestcut;
	}
}
