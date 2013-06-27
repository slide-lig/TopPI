package fr.liglab.mining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.internals.FrequentsIteratorRenamer;
import fr.liglab.mining.io.FileCollector;
import fr.liglab.mining.io.MultiThreadedFileCollector;
import fr.liglab.mining.io.NullCollector;
import fr.liglab.mining.io.PatternSortCollector;
import fr.liglab.mining.io.PatternsCollector;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.io.StdOutCollector;
import fr.liglab.mining.util.MemoryPeakWatcherThread;
import fr.liglab.mining.util.ProgressWatcherThread;

/**
 * LCM implementation, based on UnoAUA04 :
 * "An Efficient Algorithm for Enumerating Closed Patterns in Transaction Databases"
 * by Takeaki Uno el. al.
 */
public class PLCM {
	final List<TopLCMThread> threads;
	private ProgressWatcherThread progressWatch;
	protected static long chrono;

	private final PatternsCollector collector;

	private final long[] globalCounters;
	
	public PLCM(PatternsCollector patternsCollector, int nbThreads) {
		if (nbThreads < 1) {
			throw new IllegalArgumentException("nbThreads has to be > 0, given " + nbThreads);
		}
		this.collector = patternsCollector;
		this.threads = new ArrayList<TopLCMThread>(nbThreads);
		this.createThreads(nbThreads);
		this.globalCounters = new long[TopLCMCounters.values().length];
	}

	void createThreads(int nbThreads) {
		for (int i = 0; i < nbThreads; i++) {
			this.threads.add(new TopLCMThread(i));
		}
	}

	public final void collect(int support, int[] pattern) {
		this.collector.collect(support, pattern);
	}

	void initializeAndStartThreads(final ExplorationStep initState) {
		for (TopLCMThread t : this.threads) {
			t.init(initState);
			t.start();
		}
	}

	/**
	 * Initial invocation
	 */
	public final void lcm(final ExplorationStep initState) {
		if (initState.pattern.length > 0) {
			collector.collect(initState.counters.transactionsCount, initState.pattern);
		}

		this.initializeAndStartThreads(initState);

		this.progressWatch = new ProgressWatcherThread(initState);
		this.progressWatch.start();

		for (TopLCMThread t : this.threads) {
			try {
				t.join();
				for (int i = 0; i < t.counters.length; i++) {
					this.globalCounters[i] += t.counters[i];
				}
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		this.progressWatch.interrupt();
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();

		builder.append("PLCM exploration (");
		builder.append(this.threads.size());
		builder.append(" threads)");

		TopLCMCounters[] counters = TopLCMCounters.values();

		for (int i = 0; i < this.globalCounters.length; i++) {
			TopLCMCounters counter = counters[i];

			builder.append(", ");
			builder.append(counter.toString());
			builder.append(':');
			builder.append(this.globalCounters[i]);
		}

		return builder.toString();
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
			ExplorationStep next = sj.next();
			
			if (next != null) {
				thief.init(sj);
				victim.lock.readLock().unlock();
				return next;
			}
		}
		victim.lock.readLock().unlock();
		return null;
	}

	/**
	 * Some classes in EnumerationStep may declare counters here. see references
	 * to PLCMThread.counters
	 */
	public enum TopLCMCounters {
		ExplorationStepInstances, ExplorationStepCatchedWrongFirstParents, FirstParentTestRejections, TopKRejections, TransactionsCompressions
	}

	public class TopLCMThread extends Thread {
		public final long[] counters;
		final ReadWriteLock lock;
		final List<ExplorationStep> stackedJobs;
		protected final int id;

		public TopLCMThread(final int id) {
			super("PLCMThread" + id);
			this.stackedJobs = new ArrayList<ExplorationStep>();
			this.id = id;
			this.lock = new ReentrantReadWriteLock();
			this.counters = new long[TopLCMCounters.values().length];
		}

		void init(ExplorationStep initState) {
			this.lock.writeLock().lock();
			this.stackedJobs.add(initState);
			this.lock.writeLock().unlock();
		}

		@Override
		public long getId() {
			return id;
		}

		@Override
		public void run() {
			// no need to readlock, this thread is the only one that can do writes
			boolean exit = false;
			while (!exit) {
				ExplorationStep sj = null;
				if (!this.stackedJobs.isEmpty()) {
					sj = this.stackedJobs.get(this.stackedJobs.size() - 1);
					
					ExplorationStep extended = sj.next();
					// iterator is finished, remove it from the stack
					if (extended == null) {
						this.lock.writeLock().lock();

						this.stackedJobs.remove(this.stackedJobs.size() - 1);
						this.counters[TopLCMCounters.ExplorationStepInstances.ordinal()]++;
						this.counters[TopLCMCounters.ExplorationStepCatchedWrongFirstParents.ordinal()] += sj
								.getCatchedWrongFirstParentCount();

						this.lock.writeLock().unlock();
					} else {
						this.lcm(extended);
					}

				} else { // our list was empty, we should steal from another thread
					ExplorationStep stolj = stealJob(this);
					if (stolj == null) {
						exit = true;
					} else {
						lcm(stolj);
					}
				}
			}
		}

		private void lcm(ExplorationStep state) {
			collect(state.counters.transactionsCount, state.pattern);

			this.lock.writeLock().lock();
			this.stackedJobs.add(state);
			this.lock.writeLock().unlock();
		}
	}
	
	public static void main(String[] args) {

		Options options = new Options();
		CommandLineParser parser = new PosixParser();

		options.addOption(
				"b",
				false,
				"(only for standalone) Benchmark mode : show mining time and drop patterns to oblivion (in which case OUTPUT_PATH is ignored)");
		options.addOption("c", true,
				"How many sockets will share a copy of the data (triggers thread affinity), defaults to all sockets share, no copy");
		options.addOption("h", false, "Show help");
		options.addOption("i", false, "(use only with -k) Outputs a single pattern for each frequent item : " +
				"the item itself and its patterns count (max=K) - given support will be item's support count");
		options.addOption("I", false, "(use only with -k) Outputs a single pattern for distinct frequent item supports S, " +
				"containing a two integers: support sum of patterns found for items having a support count of S, and pattern count - given support will be items support count");
		options.addOption("k", true, "Run in top-k-per-item mode");
		options.addOption("m", false, "Give highest memory usage after mining (instanciates a watcher thread that periodically triggers garbage collection)");
		options.addOption("r", true, "(use only with -k) path to a file giving, per line, ITEM_ID NB_PATTERNS_TO_KEEP");
		options.addOption("s", false, "Sort items in outputted patterns, in ascending order");
		options.addOption("t", true, "How many threads will be launched (defaults to your machine's processors count)");
		options.addOption("u", false, "(use only with -k) output unique patterns only");
		options.addOption("v", false, "Enable verbose mode, which logs every extension of the empty pattern");
		options.addOption("V", false, "Enable ultra-verbose mode, which logs every pattern extension (use with care: it may produce a LOT of output)");
		
		try {
			CommandLine cmd = parser.parse(options, args);

			if (cmd.getArgs().length < 2 || cmd.getArgs().length > 3 || cmd.hasOption('h')) {
				printMan(options);
			} else {
				standalone(cmd);
			}
		} catch (ParseException e) {
			printMan(options);
		}
	}

	public static void printMan(Options options) {
		String syntax = "java fr.liglab.LCM [OPTIONS] INPUT_PATH MINSUP [OUTPUT_PATH]";
		String header = "\nIf OUTPUT_PATH is missing, patterns are printed to standard output.\nOptions are :";
		String footer = "\nFor advanced tuning you may also set properties : " + 
				ExplorationStep.KEY_LONG_TRANSACTIONS_THRESHOLD + ", "
				+ ExplorationStep.KEY_VIEW_SUPPORT_THRESHOLD;

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
		
		if (cmd.hasOption('k')) {
			ExplorationStep.LCM_STYLE = false;
		}

		int nbSocketsShareCopy = 0;
		if (cmd.hasOption('c')) {
			nbSocketsShareCopy = Integer.parseInt(cmd.getOptionValue('c'));
			PLCMAffinity.bindMainThread();
		}
		
		if (cmd.hasOption('m')) {
			memoryWatch = new MemoryPeakWatcherThread();
			memoryWatch.start();
		}

		chrono = System.currentTimeMillis();
		ExplorationStep initState = new ExplorationStep(minsup, args[0]);
		chrono = System.currentTimeMillis() - chrono;
		System.err.println("Dataset loaded in " + chrono + "ms");
		
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
		
		PatternsCollector collector = instanciateCollector(cmd, outputPath, initState, nbThreads);

		PLCM miner;
		if (nbSocketsShareCopy == 0) {
			miner = new PLCM(collector, nbThreads);
		} else {
			miner = new PLCMAffinity(collector, nbThreads, nbSocketsShareCopy);
		}

		chrono = System.currentTimeMillis();
		miner.lcm(initState);
		chrono = System.currentTimeMillis() - chrono;

		long outputted = collector.close();
		
		System.err.println(miner.toString() + 
				" // mined in " + chrono + 
				"ms // outputted " + outputted +
				" patterns // average length = "+ collector.getAveragePatternLength());
		
		if (memoryWatch != null) {
			memoryWatch.interrupt();
			System.err.println("maxUsedMemory = "+memoryWatch.getMaxUsedMemory()+"bytes");
		}
	}

	/**
	 * Parse command-line arguments to instanciate the right collector
	 * @param nbThreads 
	 */
	private static PatternsCollector instanciateCollector(CommandLine cmd, String outputPath, 
			ExplorationStep initState, int nbThreads) {

		PatternsCollector collector = null;

		if (cmd.hasOption('b')) { // BENCHMARK MODE !
			collector = new NullCollector();
		} else {
			if (outputPath != null) {
				try {
					if (cmd.hasOption('k')) {
						collector = new FileCollector(outputPath);
					} else {
						collector = new MultiThreadedFileCollector(outputPath, nbThreads);
					}
				} catch (IOException e) {
					e.printStackTrace(System.err);
					System.err.println("Aborting mining.");
					System.exit(1);
				}
			} else {
				collector = new StdOutCollector();
			}
			
			if (cmd.hasOption('s')) {
				collector = new PatternSortCollector(collector);
			}
		}
		
		if (cmd.hasOption('k')) {
			int k = Integer.parseInt(cmd.getOptionValue('k'));

			FrequentsIteratorRenamer extensions = new FrequentsIteratorRenamer(
					initState.counters.getExtensionsIterator(), initState.counters.getReverseRenaming());

			PerItemTopKCollector topKcoll = new PerItemTopKCollector(collector, k, initState.counters.nbFrequents,
					extensions);
			
			topKcoll.setInfoMode(cmd.hasOption('i'));
			topKcoll.setSupportInfoMode(cmd.hasOption('I'));
			topKcoll.setOutputUniqueOnly(cmd.hasOption('u'));
			
			if (cmd.hasOption('r')){
				topKcoll.readPerItemKFrom(cmd.getOptionValue('r'));
			}

			initState.appendSelector(topKcoll.asSelector());
			collector = topKcoll;
		}

		return collector;
	}
}
