package fr.liglab.lcm;

import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.Itemsets;
import fr.liglab.lcm.io.PatternsCollector;
import gnu.trove.iterator.TIntIterator;

/**
 * LCM implementation, based on 
 * UnoAUA04 : "An Efficient Algorithm for Enumerating Closed Patterns in Transaction Databases"
 * by Takeaki Uno el. al.
 */
public class LCM {
	PatternsCollector collector;
	
	public LCM(PatternsCollector patternsCollector) {
		collector = patternsCollector;
	}
	
	/**
	 * Initial invocation
	 */
	public void lcm (Dataset dataset) {
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
	public void lcm(int[] pattern, Dataset parent_dataset, int extension) {
		
		Dataset dataset = parent_dataset.getProjection(extension);
		int[] Q = Itemsets.extend(pattern, extension, dataset.getDiscoveredClosureItems());
		
		collector.collect(dataset.getTransactionsCount(), Q);
		
		TIntIterator iterator = dataset.getCandidatesIterator();
		while (iterator.hasNext()) {
			lcm(Q, dataset, iterator.next());
		}
	}
}
