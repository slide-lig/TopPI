package fr.liglab.mining.internals.tidlist;

import fr.liglab.mining.internals.Counters;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.iterator.TShortIterator;
import gnu.trove.list.TShortList;
import gnu.trove.list.array.TShortArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

public class ShortMapTidList extends TidList {

	public static boolean compatible(int maxTid) {
		return maxTid <= Short.MAX_VALUE;
	}

	private TIntObjectMap<TShortList> occurrences = new TIntObjectHashMap<TShortList>();

	public ShortMapTidList(Counters c) {
		this(c.distinctTransactionsCounts);
	}

	public ShortMapTidList(final int[] lengths) {
		for (int i = 0; i < lengths.length; i++) {
			if (lengths[i] > 0) {
				this.occurrences.put(i, new TShortArrayList(lengths[i]));
			}
		}
	}

	@Override
	public TidList clone() {
		ShortMapTidList o = (ShortMapTidList) super.clone();
		o.occurrences = new TIntObjectHashMap<TShortList>(this.occurrences.size());
		TIntObjectIterator<TShortList> iter = this.occurrences.iterator();
		while (iter.hasNext()) {
			iter.advance();
			o.occurrences.put(iter.key(), new TShortArrayList(iter.value()));
		}
		return o;
	}

	@Override
	public TIntIterator get(final int item) {
		final TShortList l = this.occurrences.get(item);
		if (l == null) {
			throw new IllegalArgumentException("item " + item + " has no tidlist");
		} else {
			final TShortIterator iter = l.iterator();
			return new TIntIterator() {

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}

				@Override
				public boolean hasNext() {
					return iter.hasNext();
				}

				@Override
				public int next() {
					return iter.next();
				}
			};
		}
	}

	@Override
	public TIntIterable getIterable(int item) {
		final TShortList l = this.occurrences.get(item);
		if (l == null) {
			throw new IllegalArgumentException("item " + item + " has no tidlist");
		} else {
			return new TIntIterable() {

				@Override
				public TIntIterator iterator() {
					final TShortIterator iter = l.iterator();
					return new TIntIterator() {

						@Override
						public void remove() {
							throw new UnsupportedOperationException();
						}

						@Override
						public boolean hasNext() {
							return iter.hasNext();
						}

						@Override
						public int next() {
							return iter.next();
						}
					};
				}
			};
		}
	}

	@Override
	public void addTransaction(final int item, int transaction) {
		if (transaction > Short.MAX_VALUE) {
			throw new IllegalArgumentException(transaction + " too big for a short");
		}
		TShortList l = this.occurrences.get(item);
		if (l == null) {
			throw new IllegalArgumentException("item " + item + " has no tidlist");
		}
		l.add((short) transaction);
	}

}
