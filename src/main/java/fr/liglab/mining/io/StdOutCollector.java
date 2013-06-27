package fr.liglab.mining.io;

import java.util.Arrays;

public class StdOutCollector implements PatternsCollector {

	protected long collected = 0;
	protected long collectedLength = 0;

	synchronized public void collect(final int support, final int[] pattern) {
		System.out.println(Integer.toString(support) + "\t" + Arrays.toString(pattern));
		this.collected++;
		this.collectedLength += pattern.length;
	}

	public long close() {
		return this.collected;
	}

	public int getAveragePatternLength() {
		if (this.collected == 0) {
			return 0;
		} else {
			return (int) (this.collectedLength / this.collected);
		}
	}
}
