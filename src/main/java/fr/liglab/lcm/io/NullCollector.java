package fr.liglab.lcm.io;

import fr.liglab.lcm.io.PatternsCollector;

/**
 * The collector that doesn't care at all about outputting
 */
public class NullCollector extends PatternsCollector {

	@Override
	public void collect(int support, int[] pattern) {
		
	}

	@Override
	public void close() {
		
	}

}
