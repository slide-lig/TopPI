package fr.liglab.lcm.internals.tidlist;

import gnu.trove.iterator.TIntIterator;

public abstract class TidList {

	// FIXME - is it useless ?
	abstract public TIntIterator get(final int item);

	abstract public TIntIterable getIterable(final int item);

	abstract public void addTransaction(final int item, final int transaction);

	public interface TIntIterable {
		public TIntIterator iterator();
	}

	public String toString(int[] items) {
		StringBuilder sb = new StringBuilder("[");
		boolean first = true;
		for (int item : items) {
			TIntIterator iter = this.get(item);
			if (first) {
				first = false;
			} else {
				sb.append("\n");
			}
			sb.append(item + " {");
			while (iter.hasNext()) {
				sb.append(iter.next() + ",");
			}
			sb.append("}");
		}
		sb.append("]");
		return sb.toString();
	}

	public static void main(String[] args) {
		int[] items = new int[] { 1, 2, 3, 5, Short.MAX_VALUE * 2 - 2, Short.MAX_VALUE * 2 - 1, Short.MAX_VALUE * 2 };
		int[] freqs = new int[Short.MAX_VALUE * 2 + 1];
		freqs[1] = 3;
		freqs[2] = 3;
		freqs[3] = 2;
		freqs[5] = 3;
		freqs[Short.MAX_VALUE * 2 - 2] = 1;
		freqs[Short.MAX_VALUE * 2 - 1] = 2;
		freqs[Short.MAX_VALUE * 2] = 2;
		TidList tl = new ShortMapTidList(freqs);
		tl.addTransaction(1, 1);
		tl.addTransaction(1, Short.MAX_VALUE * 2 - 2);
		tl.addTransaction(1, Short.MAX_VALUE * 2);
		tl.addTransaction(2, 1);
		tl.addTransaction(2, Short.MAX_VALUE * 2 - 2);
		tl.addTransaction(2, Short.MAX_VALUE * 2);
		tl.addTransaction(3, 1);
		tl.addTransaction(3, 12);
		tl.addTransaction(5, 1);
		tl.addTransaction(5, 58);
		tl.addTransaction(5, 57887);
		tl.addTransaction(Short.MAX_VALUE * 2 - 2, 4);
		tl.addTransaction(Short.MAX_VALUE * 2 - 1, 18);
		tl.addTransaction(Short.MAX_VALUE * 2 - 1, 27);
		tl.addTransaction(Short.MAX_VALUE * 2, 37);
		tl.addTransaction(Short.MAX_VALUE * 2, 78);
		System.out.println(tl.toString(items));
	}
}
