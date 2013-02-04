package fr.liglab.lcm;

import java.util.Collection;

import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.Itemsets;
import fr.liglab.lcm.io.PatternsCollector;

/**
 * LCM implementation, based on 
 * UnoAUA04 : "An Efficient Algorithm for Enumerating Closed Patterns in Transaction Databases"
 * by Takeaki Uno el. al.
 */
public class LCM {
	Long minsup;
	PatternsCollector collector;
	
	public LCM(Long minimumSupport, PatternsCollector patternsCollector) {
		minsup = minimumSupport;
		collector = patternsCollector;
	}
	
	public void lcm (Dataset dataset) {
		int[] pattern = dataset.getDiscoveredClosureItems(); // usually, it's empty
		
		if (pattern.length > 0) {
			collector.collect(dataset.getTransactionsCount(), pattern);
		}
		
		lcm(pattern, dataset, Integer.MAX_VALUE);
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
	public void lcm(Itemset pattern, Dataset parent_dataset, int extension) {
		
		Dataset dataset = parent_dataset.getProjection(extension);
		int[] Q = Itemsets.extend(pattern, extension, dataset.getDiscoveredClosureItems());
		
		collector.collect(dataset.getTransactionsCount(), Q);
		
		Collection<Integer> candidates= = dataset.getFrequentsAbove(extension);
		candidates.removeAll(Q); // just in case
		
		foreach(Integer candidate : candidates) {
			// Fast Prefix-Preservation Check :
			// checks that no item in ] extension; candidate [ has the same support as candidate
			if (dataset.fastPPC(extension, candidate)) {
				// we've got a winner
				lcm(Q, dataset, candidate);
			}
		}
	}
}
