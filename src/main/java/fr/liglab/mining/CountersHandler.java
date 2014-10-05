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
