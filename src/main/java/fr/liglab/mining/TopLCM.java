package fr.liglab.mining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.xml.ws.Holder;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.GenericOptionsParser;
import org.omg.CORBA.IntHolder;

import fr.liglab.mining.CountersHandler.TopLCMCounters;
import fr.liglab.mining.internals.Counters;
import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.internals.FrequentsIteratorRenamer;
import fr.liglab.mining.io.FileCollector;
import fr.liglab.mining.io.FileCollectorWithIDMapper;
import fr.liglab.mining.io.NullCollector;
import fr.liglab.mining.io.PatternSortCollector;
import fr.liglab.mining.io.PatternsCollector;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.io.StdOutCollector;
import fr.liglab.mining.mapred.TopLCMoverHadoop;
import fr.liglab.mining.util.MemoryPeakWatcherThread;
import fr.liglab.mining.util.ProgressWatcherThread;

/**
 * LCM implementation, based on UnoAUA04 :
 * "An Efficient Algorithm for Enumerating Closed Patterns in Transaction Databases"
 * by Takeaki Uno el. al.
 */
public class TopLCM {
	final List<TopLCMThread> threads;
	private ProgressWatcherThread progressWatch;
	protected static long chrono;

	PerItemTopKCollector collector;

	final long[] globalCounters;

	public TopLCM(PerItemTopKCollector patternsCollector, int nbThreads) {
		this(patternsCollector, nbThreads, false);
	}

	public TopLCM(PerItemTopKCollector patternsCollector, int nbThreads, boolean launchProgressWatch) {
		if (nbThreads < 1) {
			throw new IllegalArgumentException("nbThreads has to be > 0, given " + nbThreads);
		}
		this.collector = patternsCollector;
		this.threads = new ArrayList<TopLCMThread>(nbThreads);
		PreparedJobs pj = new PreparedJobs();
		for (int i = 0; i < nbThreads; i++) {
			this.threads.add(new TopLCMThread(pj));
		}

		this.globalCounters = new long[TopLCMCounters.values().length];
		this.progressWatch = launchProgressWatch ? new ProgressWatcherThread() : null;
	}

	@SuppressWarnings("rawtypes")
	public void setHadoopContext(Context context) {
		if (this.progressWatch != null) {
			this.progressWatch.setHadoopContext(context);
		}
	}

	/**
	 * Initial invocation for common folks
	 */
	public final void lcm(final ExplorationStep initState) {
		ExecutorService pool = Executors.newFixedThreadPool(this.threads.size());
		this.lcm(initState, pool);
		pool.shutdown();
	}

	/**
	 * Initial invocation for the hard to schedule
	 */
	public final void lcm(final ExplorationStep initState, ExecutorService pool) {
		if (initState.counters.getPattern().length > 0) {
			collector.collect(initState.counters.getTransactionsCount(), initState.counters.getPattern());
		}

		List<Future<?>> running = new ArrayList<Future<?>>(this.threads.size());

		for (TopLCMThread t : this.threads) {
			t.init(initState);
			running.add(pool.submit(t));
		}

		if (this.progressWatch != null) {
			this.progressWatch.setStartersIterator(initState.candidates);
			this.progressWatch.start();
		}

		for (Future<?> t : running) {
			try {
				t.get();
			} catch (InterruptedException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			} catch (ExecutionException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

		Arrays.fill(this.globalCounters, 0);

		for (TopLCMThread t : this.threads) {
			for (int i = 0; i < t.counters.length; i++) {
				this.globalCounters[i] += t.counters[i];
			}
		}

		if (this.progressWatch != null) {
			this.progressWatch.interrupt();
		}
	}

	public Map<TopLCMCounters, Long> getCounters() {
		HashMap<TopLCMCounters, Long> map = new HashMap<TopLCMCounters, Long>();

		TopLCMCounters[] counters = TopLCMCounters.values();

		for (int i = 0; i < this.globalCounters.length; i++) {
			map.put(counters[i], this.globalCounters[i]);
		}

		return map;
	}

	public String toString(Map<String, Long> additionalCounters) {
		StringBuilder builder = new StringBuilder();

		builder.append("{\"name\":\"TopLCM\", \"threads\":");
		builder.append(this.threads.size());

		TopLCMCounters[] counters = TopLCMCounters.values();

		for (int i = 0; i < this.globalCounters.length; i++) {
			TopLCMCounters counter = counters[i];

			builder.append(", \"");
			builder.append(counter.toString());
			builder.append("\":");
			builder.append(this.globalCounters[i]);
		}

		if (additionalCounters != null) {
			for (Entry<String, Long> entry : additionalCounters.entrySet()) {
				builder.append(", \"");
				builder.append(entry.getKey());
				builder.append("\":");
				builder.append(entry.getValue());
			}
		}

		builder.append('}');

		return builder.toString();
	}

	public String toString() {
		return this.toString(null);
	}

	ExplorationStep stealJob(TopLCMThread thief) {
		// here we need to readlock because the owner thread can write
		for (TopLCMThread victim : this.threads) {
			if (victim != thief) {
				ExplorationStep e = stealJob(thief, victim);
				if (e != null) {
					return e;
				}
			}
		}
		return null;
	}

	ExplorationStep stealJob(TopLCMThread thief, TopLCMThread victim) {
		victim.lock.readLock().lock();
		for (int stealPos = 0; stealPos < victim.stackedJobs.size(); stealPos++) {
			ExplorationStep sj = victim.stackedJobs.get(stealPos);
			ExplorationStep next = sj.next(this.collector);

			if (next != null) {
				thief.stackState(sj);
				victim.lock.readLock().unlock();
				return next;
			}
		}
		victim.lock.readLock().unlock();
		return null;
	}

	private static class CandidateCounters implements Comparable<CandidateCounters> {
		private final int candidate;
		private final Counters counters;
		private final int newCandidateBound;

		private CandidateCounters(int item, Counters counters, int newCandidateBound) {
			super();
			this.candidate = item;
			this.counters = counters;
			this.newCandidateBound = newCandidateBound;
		}

		public final int getCandidate() {
			return candidate;
		}

		public final Counters getCounters() {
			return counters;
		}

		@Override
		public int compareTo(CandidateCounters ic) {
			return this.candidate - ic.candidate;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + candidate;
			return result;
		}

		public final int getNewCandidateBound() {
			return newCandidateBound;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			CandidateCounters other = (CandidateCounters) obj;
			if (candidate != other.candidate)
				return false;
			return true;
		}

	}

	private static class StopPreparingJobsException extends Exception {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

	}

	private static class StopResumingJobsException extends Exception {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

	}

	private static class PreparedJobs {
		private Queue<CandidateCounters> stackedEs;
		private int nextConsumable;
		private int minBoundToNextConsumable;

		public PreparedJobs() {
			this.stackedEs = new PriorityQueue<TopLCM.CandidateCounters>();
			this.minBoundToNextConsumable = Integer.MAX_VALUE;
		}

		public synchronized CandidateCounters getTask(IntHolder boundHolder) throws StopPreparingJobsException,
				StopResumingJobsException {
			while (!stackedEs.isEmpty() && stackedEs.peek().getCandidate() == this.nextConsumable) {
				this.nextConsumable++;
				CandidateCounters next = stackedEs.poll();
				if (next.getNewCandidateBound() > 0) {
					this.minBoundToNextConsumable = Math
							.min(this.minBoundToNextConsumable, next.getNewCandidateBound());
				}
				if (next.counters != null) {
					boundHolder.value = this.minBoundToNextConsumable;
					return next;
				}
			}
			if (this.nextConsumable >= ExplorationStep.INSERT_UNCLOSED_UP_TO_ITEM) {
				if (this.stackedEs.isEmpty()) {
					throw new StopResumingJobsException();
				} else {
					throw new StopPreparingJobsException();
				}
			}
			boundHolder.value = this.minBoundToNextConsumable;
			return null;
		}

		public synchronized void pushTask(CandidateCounters t) {
			this.stackedEs.add(t);
		}

	}

	public class TopLCMThread implements Runnable {
		private long[] counters = null;
		private PreparedJobs preparedJobs;
		final ReadWriteLock lock;
		final List<ExplorationStep> stackedJobs;
		final IntHolder candidateHolder = new IntHolder();
		final IntHolder boundHolder = new IntHolder();
		private ExplorationStep rootState;

		public TopLCMThread(PreparedJobs preparedJobs) {
			this.stackedJobs = new ArrayList<ExplorationStep>();
			this.lock = new ReentrantReadWriteLock();
			this.preparedJobs = preparedJobs;
		}

		public void init(ExplorationStep initState) {
			this.stackState(initState);
			this.rootState = initState;
		}

		@Override
		public void run() {
			// no need to readlock, this thread is the only one that can do
			// writes
			boolean exit = false;
			boolean prepareJobs = true;
			boolean resumeJobs = true;
			while (!exit) {
				if (!this.stackedJobs.isEmpty()) {
					if (resumeJobs && this.stackedJobs.size() == 1) {
						CandidateCounters iex = null;
						try {
							iex = this.preparedJobs.getTask(this.boundHolder);
						} catch (StopPreparingJobsException e) {
							prepareJobs = false;
						} catch (StopResumingJobsException e) {
							prepareJobs = false;
							resumeJobs = false;
						}
						if (iex != null) {
							this.stackState(this.rootState.resumeExploration(iex.getCounters(), iex.getCandidate(),
									collector, this.boundHolder.value));
							continue;
						}
					}
					ExplorationStep sj = null;
					sj = this.stackedJobs.get(this.stackedJobs.size() - 1);
					if (prepareJobs && this.stackedJobs.size() == 1) {
						Counters preprocessed = sj.nextPreprocessed(collector, this.candidateHolder, this.boundHolder);
						if (this.candidateHolder.value == -1) {
							this.lock.writeLock().lock();
							this.stackedJobs.remove(this.stackedJobs.size() - 1);
							this.lock.writeLock().unlock();
						} else {
							this.preparedJobs.pushTask(new CandidateCounters(this.candidateHolder.value, preprocessed,
									this.boundHolder.value));
						}
					} else {
						ExplorationStep extended = sj.next(collector);
						// iterator is finished, remove it from the stack
						if (extended == null) {
							this.lock.writeLock().lock();
							this.stackedJobs.remove(this.stackedJobs.size() - 1);
							this.lock.writeLock().unlock();
						} else {
							this.stackState(extended);
						}
					}
				} else { // our list was empty, we should steal from another
							// thread
					prepareJobs = false;
					ExplorationStep stolj = stealJob(this);
					if (stolj == null) {
						exit = true;
					} else {
						stackState(stolj);
					}
				}
			}
			this.counters = CountersHandler.getAll();
		}

		private void stackState(ExplorationStep state) {
			CountersHandler.increment(TopLCMCounters.PatternsTraversed);
			this.lock.writeLock().lock();
			this.stackedJobs.add(state);
			this.lock.writeLock().unlock();
		}

		/**
		 * null until run() completed
		 */
		long[] getCounters() {
			return this.counters;
		}
	}

	public static void main(String[] args) throws Exception {

		Options options = new Options();
		CommandLineParser parser = new PosixParser();

		options.addOption(
				"b",
				false,
				"(only for standalone) Benchmark mode : show mining time and drop patterns to oblivion (in which case OUTPUT_PATH is ignored)");
		options.addOption("e", false, "DEBUG ONLY - prints to stdout the raised threshold, for each starter item");
		options.addOption("g", true,
				"Enables Hadoop and gives the number of groups in which the search space will be splitted");
		options.addOption("h", false, "Show help");
		options.addOption(
				"i",
				false,
				"Outputs a single pattern for each frequent item. Given support is item's support count and pattern's items are "
						+ "the item itself, its patterns count (max=K), its patterns' supports sum and its lowest pattern support.");
		options.addOption("k", true, "The K to top-k-per-item mining");
		options.addOption(
				"m",
				false,
				"(only for standalone) Give highest memory usage after mining (instanciates a watcher thread that periodically triggers garbage collection)");
		options.addOption(
				"p",
				true,
				"Comma-separated frequency thresholds that should be used for pre-filtered datasets. Warning: we create a thread for each.");
		options.addOption("r", true, "path to a file giving, per line, ITEM_ID NB_PATTERNS_TO_KEEP");
		options.addOption("s", false, "Sort items in outputted patterns, in ascending order");
		options.addOption("S", false, "(only for standalone) enable arbitrary strings as item IDs in the input file");
		options.addOption("t", true, "How many threads will be launched (defaults to your machine's processors count)");
		options.addOption("u", false, "(only for standalone) output unique patterns only");
		options.addOption("v", false, "Enable verbose mode, which logs every extension of the empty pattern");
		options.addOption("V", false,
				"Enable ultra-verbose mode, which logs every pattern extension (use with care: it may produce a LOT of output)");

		try {
			GenericOptionsParser hadoopCmd = new GenericOptionsParser(args);
			CommandLine cmd = parser.parse(options, hadoopCmd.getRemainingArgs());

			if (cmd.getArgs().length < 2 || cmd.getArgs().length > 3 || cmd.hasOption('h')) {
				printMan(options);
			} else if (!cmd.hasOption('k')) {
				System.err.println("-k parameter is mandatory");
				System.exit(1);
			} else if (cmd.hasOption('g')) {
				hadoop(cmd, hadoopCmd.getConfiguration());
			} else {
				standalone(cmd);
			}
		} catch (ParseException e) {
			printMan(options);
		}
	}

	public static void printMan(Options options) {
		String syntax = "java fr.liglab.mining.TopLCM [OPTIONS] INPUT_PATH MINSUP [OUTPUT_PATH]";
		String header = "\nIf OUTPUT_PATH is missing, patterns are printed to standard output.\nOptions are :";
		String footer = "\nFor advanced tuning you may also set properties : "
				+ ExplorationStep.KEY_LONG_TRANSACTIONS_THRESHOLD + ", " + ExplorationStep.KEY_VIEW_SUPPORT_THRESHOLD;

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(80, syntax, header, options, footer);
	}

	public static void standalone(CommandLine cmd) {
		String[] args = cmd.getArgs();
		int minsup = Integer.parseInt(args[1]);
		MemoryPeakWatcherThread memoryWatch = null;

		String outputPath = null;
		if (args.length >= 3) {
			outputPath = args[2];
		}

		if (cmd.hasOption('m')) {
			memoryWatch = new MemoryPeakWatcherThread();
			memoryWatch.start();
		}
		
		Holder<Map<String,Integer>> itemIDmap = null;
		if (cmd.hasOption('S')) {
			itemIDmap = new Holder<Map<String,Integer>>();
		}

		ExplorationStep.LOG_EPSILONS = cmd.hasOption('e');
		int k = Integer.parseInt(cmd.getOptionValue('k'));

		chrono = System.currentTimeMillis();
		ExplorationStep initState = new ExplorationStep(minsup, args[0], k, itemIDmap);
		long loadingTime = System.currentTimeMillis() - chrono;
		System.err.println("Dataset loaded in " + loadingTime + "ms");

		if (cmd.hasOption('V')) {
			ExplorationStep.verbose = true;
			ExplorationStep.ultraVerbose = true;
		} else if (cmd.hasOption('v')) {
			ExplorationStep.verbose = true;
		}

		int nbThreads = Runtime.getRuntime().availableProcessors();
		if (cmd.hasOption('t')) {
			nbThreads = Integer.parseInt(cmd.getOptionValue('t'));
		}

		chrono = System.currentTimeMillis();

		if (cmd.hasOption('p')) {
			String[] splitted = cmd.getOptionValue('p').split(",");
			Integer[] parsed = new Integer[splitted.length];
			for (int i = 0; i < splitted.length; i++) {
				parsed[i] = Integer.parseInt(splitted[i]);
			}
			initState.datasetProvider.preFilter(initState, parsed);
		}

		PerItemTopKCollector collector = instanciateCollector(cmd, outputPath, initState, nbThreads, itemIDmap);

		TopLCM miner = new TopLCM(collector, nbThreads, true);
		miner.lcm(initState);
		chrono = System.currentTimeMillis() - chrono;

		Map<String, Long> additionalCounters = new HashMap<String, Long>();
		additionalCounters.put("miningTime", chrono);
		additionalCounters.put("outputtedPatterns", collector.close());
		additionalCounters.put("loadingTime", loadingTime);
		additionalCounters.put("avgPatternLength", (long) collector.getAveragePatternLength());
		additionalCounters.put("concatenatedPatternsLength", (long) collector.getCollectedLength());

		if (memoryWatch != null) {
			memoryWatch.interrupt();
			additionalCounters.put("maxUsedMemory", memoryWatch.getMaxUsedMemory());
		}

		System.err.println(miner.toString(additionalCounters));
	}

	/**
	 * Parse command-line arguments to instanciate the right collector
	 * 
	 * @param nbThreads
	 * @param itemIDmap 
	 */
	private static PerItemTopKCollector instanciateCollector(CommandLine cmd, String outputPath,
			ExplorationStep initState, int nbThreads, Holder<Map<String, Integer>> itemIDmapHolder) {

		PerItemTopKCollector topKcoll = null;
		PatternsCollector collector = null;

		if (cmd.hasOption('b')) { // BENCHMARK MODE !
			collector = new NullCollector();
		} else {
			Map<Integer, String> itemIDmap = null;
			if (itemIDmapHolder != null) {
				itemIDmap = new HashMap<Integer, String>(itemIDmapHolder.value.size());
				for (Entry<String, Integer> entry : itemIDmapHolder.value.entrySet()) {
					itemIDmap.put(entry.getValue(), entry.getKey());
				}
			}
			
			if (outputPath != null) {
				try {
					if (itemIDmap == null) {
						collector = new FileCollector(outputPath);
					} else {
						collector = new FileCollectorWithIDMapper(outputPath, itemIDmap);
					}
					
				} catch (IOException e) {
					e.printStackTrace(System.err);
					System.err.println("Aborting mining.");
					System.exit(1);
				}
			} else {
				collector = new StdOutCollector(itemIDmap);
			}

			if (cmd.hasOption('s')) {
				collector = new PatternSortCollector(collector);
			}
		}

		int k = Integer.parseInt(cmd.getOptionValue('k'));

		// breadth is 0 because we're at the root, so there is no point in
		// having some
		FrequentsIteratorRenamer extensions = new FrequentsIteratorRenamer(initState.counters.getExtensionsIterator(),
				initState.counters.getReverseRenaming());

		topKcoll = new PerItemTopKCollector(collector, k, initState.counters.getNbFrequents(), extensions);

		topKcoll.setInfoMode(cmd.hasOption('i'));
		topKcoll.setOutputUniqueOnly(cmd.hasOption('u'));

		if (cmd.hasOption('r')) {
			topKcoll.readPerItemKFrom(cmd.getOptionValue('r'));
		}

		initState.appendSelector(topKcoll.asSelector());

		return topKcoll;
	}

	public static void hadoop(CommandLine cmd, Configuration conf) throws Exception {
		String[] args = cmd.getArgs();

		if (args.length != 3) {
			throw new IllegalArgumentException("Output's prefix path must be provided when using Hadoop");
		}

		conf.set(TopLCMoverHadoop.KEY_INPUT, args[0]);
		conf.setInt(TopLCMoverHadoop.KEY_MINSUP, Integer.parseInt(args[1]));
		conf.set(TopLCMoverHadoop.KEY_OUTPUT, args[2]);
		conf.setInt(TopLCMoverHadoop.KEY_K, Integer.parseInt(cmd.getOptionValue('k')));
		conf.setInt(TopLCMoverHadoop.KEY_NBGROUPS, Integer.parseInt(cmd.getOptionValue('g')));

		if (cmd.hasOption('s')) {
			System.err.println("Hadoop version does not support itemset sorting");
			System.exit(1);
		}
		if (cmd.hasOption('v')) {
			conf.setBoolean(TopLCMoverHadoop.KEY_VERBOSE, true);
		}
		if (cmd.hasOption('V')) {
			conf.setBoolean(TopLCMoverHadoop.KEY_ULTRA_VERBOSE, true);
		}

		TopLCMoverHadoop driver = new TopLCMoverHadoop(conf);
		System.exit(driver.run());
	}
}
