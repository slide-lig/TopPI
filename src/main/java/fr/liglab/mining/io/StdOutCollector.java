package fr.liglab.mining.io;

import java.util.Arrays;
import java.util.Map;

public final class StdOutCollector implements PatternsCollector {

	protected long collected = 0;
	protected long collectedLength = 0;
	private Map<Integer, String> map;
	
	/**
	 * @param itemIDmap can be null
	 */
	public StdOutCollector(Map<Integer, String> itemIDmap) {
		this.map = itemIDmap;
	}

	synchronized public void collect(final int support, final int[] pattern) {
		if (map == null) {
			System.out.println(Integer.toString(support) + "\t" + Arrays.toString(pattern));
		} else {
			StringBuilder sb = new StringBuilder(2*pattern.length + 1);
			sb.append(support);
			char separator = '\t';
			for (int i : pattern) {
				sb.append(separator);
				sb.append(map.get(i));
				separator = ' ';
			}
			System.out.println(sb.toString());
		}
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

	@Override
	public long getCollected() {
		return this.collected;
	}

	@Override
	public long getCollectedLength() {
		return this.collectedLength;
	}
}
