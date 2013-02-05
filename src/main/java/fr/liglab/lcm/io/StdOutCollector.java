package fr.liglab.lcm.io;

import java.util.Arrays;

public class StdOutCollector implements PatternsCollector {

	public void collect(int support, int[] pattern) {
		System.out.println(Integer.toString(support) + "\t" + Arrays.toString(pattern));
	}
	
	public void close() {
		
	}
}
