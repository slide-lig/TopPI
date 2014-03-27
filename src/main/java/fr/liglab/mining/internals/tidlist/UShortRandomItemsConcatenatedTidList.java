package fr.liglab.mining.internals.tidlist;

import java.util.Arrays;

import fr.liglab.mining.internals.Counters;

public class UShortRandomItemsConcatenatedTidList extends RandomItemsConcatenatedTidList {

	public static boolean compatible(int maxTid) {
		return maxTid <= Character.MAX_VALUE;
	}

	private char[] array;

	@Override
	public TidList clone() {
		UShortRandomItemsConcatenatedTidList o = (UShortRandomItemsConcatenatedTidList) super.clone();
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
			throw new IllegalArgumentException(transaction + " too big for a short");
		}
		this.array[position] = (char) transaction;
	}

	@Override
	int read(int position) {
		return this.array[position];

	}

	public UShortRandomItemsConcatenatedTidList(Counters c) {
		super(c);
	}
}
