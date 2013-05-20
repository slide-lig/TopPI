package fr.liglab.lcm.internals.tidlist;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TShortIterator;
import gnu.trove.list.TShortList;
import gnu.trove.list.array.TShortArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

public class ShortMapTidList implements TidList {

	private final TIntObjectMap<TShortList> occurrences = new TIntObjectHashMap<TShortList>();
	
	public ShortMapTidList(final TIntIntMap lengths) {
		TIntIntIterator iter = lengths.iterator();
		while (iter.hasNext()) {
			iter.advance();
			this.occurrences.put(iter.key(), new TShortArrayList(iter.value()));
		}
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
					iter.remove();
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
		return new TIntIterable() {

			@Override
			public TIntIterator iterator() {
				final TShortIterator iter = l.iterator();
				return new TIntIterator() {

					@Override
					public void remove() {
						iter.remove();
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

	@Override
	public void addTransaction(final int item, final int transaction) {
		if (transaction > Short.MAX_VALUE) {
			throw new IllegalArgumentException(transaction + " too big for a short");
		}
		TShortList l = this.occurrences.get(item);
		if (l == null) {
			l = new TShortArrayList();
			this.occurrences.put(item, l);
		}
		l.add((short) item);
	}

}
