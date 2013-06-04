package fr.liglab.lcm.internals.tidlist;

import java.util.Arrays;

import fr.liglab.lcm.internals.Counters;

public class ShortConsecutiveItemsConcatenatedTidList extends ConsecutiveItemsConcatenatedTidList {

	public static boolean compatible(int maxTid) {
		return maxTid <= Short.MAX_VALUE;
	}

	private short[] array;

	@Override
	public TidList clone() {
		ShortConsecutiveItemsConcatenatedTidList o = (ShortConsecutiveItemsConcatenatedTidList) super.clone();
		o.array = Arrays.copyOf(this.array, this.array.length);
		return o;
	}

	@Override
	void allocateArray(int size) {
		this.array = new short[size];
	}

	@Override
	void write(int position, int transaction) {
		if (transaction > Short.MAX_VALUE) {
			throw new IllegalArgumentException(transaction + " too big for a short");
		}
		this.array[position] = (short) transaction;
	}

	@Override
	int read(int position) {
		return this.array[position];
	}

	public ShortConsecutiveItemsConcatenatedTidList(Counters c, int highestTidList) {
		super(c, highestTidList);
	}

	public ShortConsecutiveItemsConcatenatedTidList(int[] lengths, int highestTidList) {
		super(lengths, highestTidList);
	}

}
