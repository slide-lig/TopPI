package fr.liglab.mining;

public final class CountersHandler {
	/**
	 * Some classes in EnumerationStep may declare counters here. see references
	 * to TopLCMCounters.counters
	 */
	public enum TopLCMCounters {
		FailedFPTests, PreFPTestsRejections, TopKRejections, TransactionsCompressions, NbDatasets, NbDatasetViews, NbCounters, PatternsTraversed, EjectedPlaceholders, EjectedPatterns, DatasetReductionByEpsilonRaising
	}

	private static final ThreadLocal<long[]> counters = new ThreadLocal<long[]>() {
		@Override
		protected long[] initialValue() {
			return new long[TopLCMCounters.values().length];
		}
	};

	public static long[] getAll() {
		return counters.get();
	}

	public static long get(TopLCMCounters counter) {
		return counters.get()[counter.ordinal()];
	}

	public static void increment(TopLCMCounters counter) {
		counters.get()[counter.ordinal()]++;
	}

	public static void add(TopLCMCounters counter, long delta) {
		counters.get()[counter.ordinal()] += delta;
	}
}
