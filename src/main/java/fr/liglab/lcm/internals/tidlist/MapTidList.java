package fr.liglab.lcm.internals.tidlist;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

public class MapTidList implements TidList {

	private final TIntObjectMap<TIntList> occurrences = new TIntObjectHashMap<TIntList>();
	
	public MapTidList(final TIntIntMap lengths) {
		TIntIntIterator iter = lengths.iterator();
		while (iter.hasNext()) {
			iter.advance();
			this.occurrences.put(iter.key(), new TIntArrayList(iter.value()));
		}
	}

	@Override
	public TIntIterator get(final int item) {
		final TIntList l = this.occurrences.get(item);
		if (l == null) {
			throw new IllegalArgumentException("item " + item + " has no tidlist");
		} else {
			return l.iterator();
		}
	}

	@Override
	public TIntIterable getIterable(int item) {
		final TIntList l = this.occurrences.get(item);
		return new TIntIterable() {

			@Override
			public TIntIterator iterator() {
				return l.iterator();
			}
		};
	}

	@Override
	public void addTransaction(final int item, final int transaction) {
		TIntList l = this.occurrences.get(item);
		if (l == null) {
			l = new TIntArrayList();
			this.occurrences.put(item, l);
		}
		l.add(item);
	}

}
