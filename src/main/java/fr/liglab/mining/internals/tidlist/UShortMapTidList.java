/*
	This file is part of TopPI - see https://github.com/slide-lig/TopPI/
	
	Copyright 2016 Martin Kirchgessner, Vincent Leroy, Alexandre Termier, Sihem Amer-Yahia, Marie-Christine Rousset, Université Grenoble Alpes, LIG, CNRS
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	 http://www.apache.org/licenses/LICENSE-2.0
	 
	or see the LICENSE.txt file joined with this program.
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package fr.liglab.mining.internals.tidlist;

import fr.liglab.mining.internals.Counters;
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
		for (int i = 0; i <= c.getMaxFrequent(); i++) {
			int count = c.getDistinctTransactionsCount(i);
			if (count > 0) {
				this.occurrences.put(i, new TCharArrayList(count));
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
