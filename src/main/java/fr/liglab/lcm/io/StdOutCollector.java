package fr.liglab.lcm.io;

import java.util.Arrays;

public class StdOutCollector implements PatternsCollector {

	public void collect(Long support, int[] pattern) {
		System.out.println(support.toString() + "\t" + Arrays.toString(pattern));
	}
	
	public void close() {
		
	}
}
