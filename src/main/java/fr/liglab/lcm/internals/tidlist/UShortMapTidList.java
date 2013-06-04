package fr.liglab.lcm.internals.tidlist;

import fr.liglab.lcm.internals.Counters;
import gnu.trove.iterator.TCharIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.TCharList;
import gnu.trove.list.array.TCharArrayList;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

public class UShortMapTidList extends TidList {

	public static boolean compatible(int maxTid) {
		return maxTid <= Character.MAX_VALUE;
	}

	private TIntObjectMap<TCharList> occurrences = new TIntObjectHashMap<TCharList>();

	public UShortMapTidList(Counters c) {
		this(c.distinctTransactionsCounts);
	}

	public UShortMapTidList(final int[] lengths) {
		for (int i = 0; i < lengths.length; i++) {
			if (lengths[i] > 0) {
				this.occurrences.put(i, new TCharArrayList(lengths[i]));
			}
		}
	}

	@Override
	public TidList clone() {
		UShortMapTidList o = (UShortMapTidList) super.clone();
		o.occurrences = new TIntObjectHashMap<TCharList>(this.occurrences.size());
		TIntObjectIterator<TCharList> iter = this.occurrences.iterator();
		while (iter.hasNext()) {
			iter.advance();
			o.occurrences.put(iter.key(), new TCharArrayList(iter.value()));
		}
		return o;
	}

	@Override
	public TIntIterator get(final int item) {
		final TCharList l = this.occurrences.get(item);
		if (l == null) {
			throw new IllegalArgumentException("item " + item + " has no tidlist");
		} else {
			final TCharIterator iter = l.iterator();
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
		final TCharList l = this.occurrences.get(item);
		if (l == null) {
			throw new IllegalArgumentException("item " + item + " has no tidlist");
		} else {
			return new TIntIterable() {

				@Override
				public TIntIterator iterator() {
					final TCharIterator iter = l.iterator();
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
		if (transaction > Character.MAX_VALUE) {
			throw new IllegalArgumentException(transaction + " too big for a short");
		}
		TCharList l = this.occurrences.get(item);
		if (l == null) {
			throw new IllegalArgumentException("item " + item + " has no tidlist");
		}
		l.add((char) transaction);
	}

}
