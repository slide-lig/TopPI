package fr.liglab.lcm.internals.tidlist;

import java.util.Arrays;

import fr.liglab.lcm.internals.Counters;

public class UShortConsecutiveItemsConcatenatedTidList extends ConsecutiveItemsConcatenatedTidList {

	@SuppressWarnings("cast")
	public static boolean compatible(int maxTid) {
		return maxTid <= ((int) Short.MAX_VALUE) - ((int) Short.MIN_VALUE);
	}

	private short[] array;

	@Override
	public TidList clone() {
		UShortConsecutiveItemsConcatenatedTidList o = (UShortConsecutiveItemsConcatenatedTidList) super.clone();
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
			transaction = -transaction + Short.MAX_VALUE;
			if (transaction < Short.MIN_VALUE) {
				throw new IllegalArgumentException(transaction + " too big for a short");
			}
		}
		this.array[position] = (short) transaction;
	}

	@SuppressWarnings("cast")
	@Override
	int read(int position) {
		if (this.array[position] >= 0) {
			return this.array[position];
		} else {
			return ((int) -this.array[position]) + ((int) Short.MAX_VALUE);
		}
	}

	public UShortConsecutiveItemsConcatenatedTidList(Counters c) {
		super(c);
	}

	public UShortConsecutiveItemsConcatenatedTidList(int[] lengths) {
		super(lengths);
	}

}
