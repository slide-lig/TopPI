package fr.liglab.lcm.io;

import java.util.Arrays;

public class StdOutCollector extends PatternsCollector {

	public void collect(final int support, final int[] pattern) {
		System.out.println(Integer.toString(support) + "\t" + Arrays.toString(pattern));
	}
	
	public void close() {
		
	}
}
