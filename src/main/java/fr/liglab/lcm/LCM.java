package fr.liglab.lcm;

import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.Itemset;
import fr.liglab.lcm.io.PatternsCollector;

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
