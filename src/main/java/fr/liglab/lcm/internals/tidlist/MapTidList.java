package fr.liglab.lcm.internals.tidlist;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

public class MapTidList extends TidList {

	public static boolean compatible(int maxTid) {
		return true;
	}

	private TIntObjectMap<TIntList> occurrences = new TIntObjectHashMap<TIntList>();

	public MapTidList(final int[] supports) {
		for (int i = 0; i < supports.length; i++) {
			int j = supports[i];
			if (j > 0) {
				this.occurrences.put(i, new TIntArrayList(j));
			}
		}
	}

	public MapTidList(final TIntIntMap lengths) {
		TIntIntIterator iter = lengths.iterator();
		while (iter.hasNext()) {
			iter.advance();
			this.occurrences.put(iter.key(), new TIntArrayList(iter.value()));
		}
	}

	@Override
	public TidList clone() {
		MapTidList o = (MapTidList) super.clone();
		o.occurrences = new TIntObjectHashMap<TIntList>(this.occurrences.size());
		TIntObjectIterator<TIntList> iter = this.occurrences.iterator();
		while (iter.hasNext()) {
			iter.advance();
			o.occurrences.put(iter.key(), new TIntArrayList(iter.value()));
		}
		return o;
	}

	@Override
	public String toString() {
		return this.occurrences.toString();
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
		if (l == null) {
			throw new IllegalArgumentException("item " + item + " has no tidlist");
		}
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
		l.add(transaction);
	}

}
