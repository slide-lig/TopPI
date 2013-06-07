package fr.liglab.lcm.io;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The collector that doesn't care at all about outputting
 */
public class NullCollector implements PatternsCollector {

	protected AtomicInteger collectedCount = new AtomicInteger(0);
	protected AtomicLong collectedLength = new AtomicLong(0);

	@Override
	public void collect(int support, int[] pattern) {
		this.collectedCount.incrementAndGet();
		this.collectedLength.addAndGet(pattern.length);
	}

	@Override
	public long close() {
		return this.collectedCount.get();
	}

	public int getAveragePatternLength() {
		if (this.collectedCount.get() == 0) {
			return 0;
		} else {
			return (int) (this.collectedLength.get() / this.collectedCount.get());
		}
	}
}
