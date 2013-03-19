package fr.liglab.lcm.io;

import java.util.Arrays;

public class StdOutCollector extends PatternsCollector {
	
	protected long collected = 0;

	public void collect(final int support, final int[] pattern) {
		System.out.println(Integer.toString(support) + "\t" + Arrays.toString(pattern));
		this.collected++;
	}
	
	public long close() {
		return this.collected;
	}
}
