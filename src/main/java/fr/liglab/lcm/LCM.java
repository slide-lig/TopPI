package fr.liglab.lcm;

import java.util.Collection;

import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.Itemset;
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
		Itemset pattern = dataset.getClosureExtension(); // usually, it's empty
		
		if (!pattern.isEmpty()) {
			collector.collect(dataset.getTransactionsCount(), pattern);
		}
		
		lcm(pattern, dataset, Integer.MAX_VALUE);
	}
	
	
	/**
	 * Items in patterns are written by increasing order.
	 * 
	 * This function will output clo(pattern U {extension}), and (in recursive calls) 
	 * all closed frequent closed itemsets prefixed by "pattern U {extension}"
	 * 
	 * HEAVY ASSUMPTIONS :
	 * @param pattern is a closed frequent itemset, freshly found out of :
	 * @param parent_dataset - pattern's support
	 * @param extension is an item known to yield a prefix-preserving extension of P 
	 */
	public void lcm2(Itemset pattern, Dataset parent_dataset, int extension) {
		
		// in other words, we know {P,{i}} is a frequent pattern. 
		// its closure may involve items > i
		// let's found them in {P,i}'s projected dataset
		Dataset dataset = parent_dataset.getProjection(extension);
		Itemset Q = new Itemset(pattern, extension, dataset.getDiscoveredClosureItems());
		
		collector.collect(dataset.getTransactionsCount(), Q);
		
		// from UnoAUA04-Lemma 3 : "extension" is Q's core index 
		Collection<E> candidates= = dataset.getFrequentsAbove(extension);
		candidates.removeAll(Q); // just in case
		
		foreach(int candidate : candidates) {
			// Fast Prefix-Preservation Check :
			// checks that no item in ] extension; candidate [ has the same support as candidate
			if (dataset.fastPPC(extension, candidate)) {
				// we've got a winner
				lcm3(Q, dataset, candidate);
			}
		}
	}
	
	
	/**
	 * Assumes the dataset has been recursively constructed according the given (closed) pattern
	 * 
	 * This method will recursively collect frequent closed extensions 
	 * of the given pattern
	 * In order to ensure uniqueness, no extension will involve items >= maxExtension
	 */
	public void lcm(Itemset pattern, Dataset dataset, int maxExtension) {
		
		for (int item : dataset.getFrequentItems()) {
			if (item < maxExtension) {
				
				Dataset projection = new Dataset(minsup, dataset, item);
				Itemset closureExtension = projection.getClosureExtension();
				
				// FIXME : now that we're not modifying transactions at all, 
				// pattern's items are appearing in closureExtension
				closureExtension.removeAll(pattern);
				
				if (closureExtension.isEmpty() || closureExtension.max() < item) {
					// TODO: something that won't trigger array allocations during the next two lines
					Itemset extended = new Itemset(pattern, item);
					extended.addAll(closureExtension);
					collector.collect(projection.getTransactionsCount(), extended);
					
					lcm(extended, projection, item);
				}
			}
		}
	}
}
