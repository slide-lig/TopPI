package fr.liglab.mining.internals.tidlist;

import java.util.Arrays;

import fr.liglab.mining.internals.Counters;

public class ByteRandomItemsConcatenatedTidList extends RandomItemsConcatenatedTidList {

	public static boolean compatible(int maxTid) {
		return maxTid <= Byte.MAX_VALUE;
	}

	private byte[] array;

	@Override
	public TidList clone() {
		ByteRandomItemsConcatenatedTidList o = (ByteRandomItemsConcatenatedTidList) super.clone();
		o.array = Arrays.copyOf(this.array, this.array.length);
		return o;
	}

	@Override
	void allocateArray(int size) {
		this.array = new byte[size];
	}

	@Override
	void write(int position, int transaction) {
		if (transaction > Byte.MAX_VALUE) {
			throw new IllegalArgumentException(transaction + " too big for a byte");
		}
		this.array[position] = (byte) transaction;
	}

	@Override
	int read(int position) {
		return this.array[position];
	}

	public ByteRandomItemsConcatenatedTidList(Counters c) {
		super(c);
	}

	public ByteRandomItemsConcatenatedTidList(int[] lengths) {
		super(lengths);
	}

}
