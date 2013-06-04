package fr.liglab.lcm.internals.tidlist;

import java.util.Arrays;

import fr.liglab.lcm.internals.Counters;

public class ByteConsecutiveItemsConcatenatedTidList extends ConsecutiveItemsConcatenatedTidList {

	public static boolean compatible(int maxTid) {
		return maxTid <= Byte.MAX_VALUE;
	}

	private byte[] array;

	@Override
	public TidList clone() {
		ByteConsecutiveItemsConcatenatedTidList o = (ByteConsecutiveItemsConcatenatedTidList) super.clone();
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

	public ByteConsecutiveItemsConcatenatedTidList(Counters c) {
		super(c);
	}

	public ByteConsecutiveItemsConcatenatedTidList(int[] lengths) {
		super(lengths);
	}

}
