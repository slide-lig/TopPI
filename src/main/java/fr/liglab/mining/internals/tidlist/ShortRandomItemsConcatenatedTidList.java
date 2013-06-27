package fr.liglab.mining.internals.tidlist;

import java.util.Arrays;

import fr.liglab.mining.internals.Counters;

public class ShortRandomItemsConcatenatedTidList extends RandomItemsConcatenatedTidList {

	public static boolean compatible(int maxTid) {
		return maxTid <= Short.MAX_VALUE;
	}

	private short[] array;

	@Override
	public TidList clone() {
		ShortRandomItemsConcatenatedTidList o = (ShortRandomItemsConcatenatedTidList) super.clone();
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

	public ShortRandomItemsConcatenatedTidList(Counters c) {
		super(c);
	}

	public ShortRandomItemsConcatenatedTidList(int[] lengths) {
		super(lengths);
	}

}
