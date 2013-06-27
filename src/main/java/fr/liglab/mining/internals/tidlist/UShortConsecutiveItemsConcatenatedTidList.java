package fr.liglab.mining.internals.tidlist;

import java.util.Arrays;

import fr.liglab.mining.internals.Counters;

public class UShortConsecutiveItemsConcatenatedTidList extends ConsecutiveItemsConcatenatedTidList {

	public static boolean compatible(int maxTid) {
		return maxTid <= Character.MAX_VALUE;
	}

	private char[] array;

	@Override
	public TidList clone() {
		UShortConsecutiveItemsConcatenatedTidList o = (UShortConsecutiveItemsConcatenatedTidList) super.clone();
		o.array = Arrays.copyOf(this.array, this.array.length);
		return o;
	}

	@Override
	void allocateArray(int size) {
		this.array = new char[size];
	}

	@Override
	void write(int position, int transaction) {
		if (transaction > Character.MAX_VALUE) {
			throw new IllegalArgumentException(transaction + " too big for a char");
		}
		this.array[position] = (char) transaction;
	}

	@Override
	int read(int position) {
		return this.array[position];
	}

	public UShortConsecutiveItemsConcatenatedTidList(Counters c, int highestItem) {
		super(c, highestItem);
	}

	public UShortConsecutiveItemsConcatenatedTidList(int[] lengths, int highestItem) {
		super(lengths, highestItem);
	}

}
