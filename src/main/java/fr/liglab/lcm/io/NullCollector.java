package fr.liglab.lcm.io;


/**
 * The collector that doesn't care at all about outputting
 */
public class NullCollector extends PatternsCollector {
	
	protected long collectedCount = 0;

	@Override
	public void collect(int support, int[] pattern) {
		this.collectedCount++;
	}

	@Override
	public long close() {
		return this.collectedCount;
	}

}
