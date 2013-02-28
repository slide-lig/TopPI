package fr.liglab.lcm;

import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.ExtensionsIterator;
import fr.liglab.lcm.io.PatternsCollector;
import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.TIntIntMap;

/**
 * LCM implementation, based on 
 * UnoAUA04 : "An Efficient Algorithm for Enumerating Closed Patterns in Transaction Databases"
 * by Takeaki Uno el. al.
 */
public class LCM {
	private final PatternsCollector collector;
	
	private int collected = 0;
	private int explored = 0;
	private int cutted = 0;
	
	public LCM(PatternsCollector patternsCollector) {
		collector = patternsCollector;
	}
	
	/**
	 * Initial invocation
	 */
	public void lcm (final Dataset dataset) {
		int[] pattern = dataset.getDiscoveredClosureItems(); // usually, it's empty
		
		if (pattern.length > 0) {
			collected++;
			collector.collect(dataset.getTransactionsCount(), pattern);
		}
		
		TIntIterator iterator = dataset.getCandidatesIterator();
		while (iterator.hasNext()) {
			explored++;
			lcm(pattern, dataset, iterator.next());
		}
	}
	
	
	/**
	 * This function will output clo(pattern U {extension}), and (in recursive calls) 
	 * all closed frequent closed itemsets prefixed by "pattern U {extension}"
	 * 
	 * HEAVY ASSUMPTIONS :
	 * @param pattern is a closed frequent itemset, freshly found out of :
	 * @param parent_dataset - pattern's support
	 * @param extension is an item known to yield a prefix-preserving extension of P 
	 */
	public void lcm(final int[] pattern, final Dataset parent_dataset, int extension) {
		
		Dataset dataset = parent_dataset.getProjection(extension);
		int[] Q = ItemsetsFactory.extend(pattern, extension, dataset.getDiscoveredClosureItems());
		int QSupport = dataset.getTransactionsCount();
		collector.collect(QSupport, Q);
		collected++;
		
		ExtensionsIterator iterator = dataset.getCandidatesIterator();
		int[] sortedFreqs = iterator.getSortedFrequents();
		TIntIntMap supportCounts = dataset.getSupportCounts();
		
		while (iterator.hasNext()) {
			
			int candidate = iterator.next();
			
			if (collector.explore(Q, QSupport, candidate, sortedFreqs, supportCounts)) {
				explored++;
				lcm(Q, dataset, candidate);
			} else {
				cutted++;
			}
		}
	}
	
	public String toString() {
		return collected + " patterns collected / " + explored + " explored / " + cutted + " cutted";
	}
}
