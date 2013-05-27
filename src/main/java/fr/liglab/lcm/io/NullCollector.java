package fr.liglab.lcm.io;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The collector that doesn't care at all about outputting
 */
public class NullCollector implements PatternsCollector {

	protected AtomicInteger collectedCount = new AtomicInteger(0);

	@Override
	public void collect(int support, int[] pattern) {
		this.collectedCount.incrementAndGet();
	}

	@Override
	public long close() {
		return this.collectedCount.get();
	}

}
