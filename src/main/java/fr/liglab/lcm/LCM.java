package fr.liglab.lcm;

import java.util.Arrays;

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
 */
public class LCM {
	private final PatternsCollector collector;

	private int explored = 0;
	private int explorecut = 0;
	private int pptestcut = 0;

	public LCM(PatternsCollector patternsCollector) {
		collector = patternsCollector;
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

		extensionLoop(pattern, dataset);
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

			int[] Q = ItemsetsFactory.extend(pattern, extension,
					dataset.getDiscoveredClosureItems());

			collector.collect(dataset.getTransactionsCount(), Q);

			extensionLoop(Q, dataset);
		} catch (OutOfMemoryError e) {
			if (HeapDumper.basePath != null) {
				HeapDumper.dumpThemAll();
			}
			throw new RuntimeException(
					"OutOfMemoryError while extending pattern "
							+ Arrays.toString(pattern) + " with " + extension);
		}
	}

	private void extensionLoop(final int[] pattern, final Dataset dataset) {
		ExtensionsIterator iterator = dataset.getCandidatesIterator();
		int[] sortedFreqs = iterator.getSortedFrequents();
		TIntIntMap supportCounts = dataset.getSupportCounts();

		TIntIntMap failedPPTests = new TIntIntHashMap();
		int candidate;
		int previousExplore = -1;
		int previousCandidate = -1;

		while ((candidate = iterator.getExtension()) != -1) {
			int explore = collector.explore(pattern, candidate, sortedFreqs,
					supportCounts, failedPPTests, previousCandidate,
					previousExplore);
			if (explore < 0) {
				try {
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
		}
	}

	/**
	 * This exception must be thrown by methods invoked directly or not by lcm()
	 * when they find out that their search branch is uninteresting
	 */
	public static class DontExploreThisBranchException extends Exception {
		private static final long serialVersionUID = 2969583589161047791L;

		public final int firstParent;

		/**
		 * @param foundFirstParent
		 *            a item found in closure > coreItem
		 */
		public DontExploreThisBranchException(int foundFirstParent) {
			this.firstParent = foundFirstParent;
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
