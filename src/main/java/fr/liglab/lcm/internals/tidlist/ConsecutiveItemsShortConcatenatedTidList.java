package fr.liglab.lcm.internals.tidlist;


public class ConsecutiveItemsShortConcatenatedTidList extends ShortConcatenatedTidList {

	private final int[] startPositions;

	public ConsecutiveItemsShortConcatenatedTidList(final int[] lengths) {
		int startPos = 0;
		this.startPositions = new int[lengths.length];
		for (int i = 0; i < lengths.length; i++) {
			this.startPositions[i] = startPos;
			startPos += (1 + lengths[i]);
		}
		this.concatenated = new short[startPos];
	}

	@Override
	protected int getPosition(final int item) {
		if (item >= this.startPositions.length) {
			return -1;
		} else {
			return this.startPositions[item];
		}
	}

}
