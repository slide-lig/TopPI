package fr.liglab.lcm.internals.tidlist;

import java.util.Arrays;

import fr.liglab.lcm.internals.nomaps.Counters;

public class IntConsecutiveItemsConcatenatedTidList extends ConsecutiveItemsConcatenatedTidList {

	public static boolean compatible(int maxTid) {
		return true;
	}

	private int[] array;

	@Override
	public TidList clone() {
		IntConsecutiveItemsConcatenatedTidList o = (IntConsecutiveItemsConcatenatedTidList) super.clone();
		o.array = Arrays.copyOf(this.array, this.array.length);
		return o;
	}

	@Override
	void allocateArray(int size) {
		this.array = new int[size];
	}

	@Override
	void write(int position, int transaction) {
		this.array[position] = transaction;
	}

	@Override
	int read(int position) {
		return this.array[position];
	}

	public IntConsecutiveItemsConcatenatedTidList(Counters c) {
		super(c);
	}

	public IntConsecutiveItemsConcatenatedTidList(int[] lengths) {
		super(lengths);
	}

}
