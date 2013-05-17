package fr.liglab.lcm.internals.nomaps;

/**
 * Represents an LCM recursion step
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
	

}
