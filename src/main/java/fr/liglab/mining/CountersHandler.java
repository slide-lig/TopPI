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
package fr.liglab.mining;

public final class CountersHandler {
	/**
	 * Some classes in EnumerationStep may declare counters here. see references
	 * to TopPICounters.counters
	 */
	public enum TopPICounters {
		FailedFPTests, PreFPTestsRejections, TopKRejections, TransactionsCompressions, NbDatasets, NbDatasetViews, NbCounters, NbSparseCounters, PatternsTraversed, EjectedPlaceholders, EjectedPatterns, DatasetReductionByEpsilonRaising, RedoCounters
	}

	private static final ThreadLocal<long[]> counters = new ThreadLocal<long[]>() {
		@Override
		protected long[] initialValue() {
			return new long[TopPICounters.values().length];
		}
	};

	public static long[] getAll() {
		return counters.get();
	}

	public static long get(TopPICounters counter) {
		return counters.get()[counter.ordinal()];
	}

	public static void increment(TopPICounters counter) {
		counters.get()[counter.ordinal()]++;
	}

	public static void add(TopPICounters counter, long delta) {
		counters.get()[counter.ordinal()] += delta;
	}
}
