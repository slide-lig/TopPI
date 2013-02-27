package fr.liglab.lcm;

import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.ExtensionsIterator;
import fr.liglab.lcm.io.PatternsCollector;
import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.iterator.TIntIterator;

/**
 * LCM implementation, based on 
 * UnoAUA04 : "An Efficient Algorithm for Enumerating Closed Patterns in Transaction Databases"
 * by Takeaki Uno el. al.
 */
public class LCM {
	private final PatternsCollector collector;
	
	public LCM(PatternsCollector patternsCollector) {
		collector = patternsCollector;
	}
	
	/**
	 * Initial invocation
	 */
	public void lcm (final Dataset dataset) {
		int[] pattern = dataset.getDiscoveredClosureItems(); // usually, it's empty
		
		if (pattern.length > 0) {
			collector.collect(dataset.getTransactionsCount(), pattern);
		}
		
		TIntIterator iterator = dataset.getCandidatesIterator();
		while (iterator.hasNext()) {
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
		
		collector.collect(dataset.getTransactionsCount(), Q);
		
		ExtensionsIterator iterator = dataset.getCandidatesIterator();
		while (iterator.hasNext()) {
			lcm(Q, dataset, iterator.next());
		}
	}
}
