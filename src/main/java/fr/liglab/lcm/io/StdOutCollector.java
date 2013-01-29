package fr.liglab.lcm.io;

import fr.liglab.lcm.internals.Itemset;

public class StdOutCollector implements PatternsCollector {

	public void collect(Long support, Itemset pattern) {
		System.out.println(support.toString() + "\t" + pattern.toString());
	}
	
	public void close() {
		
	}
}
