package fr.liglab.lcm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import fr.liglab.lcm.internals.ExplorationStep;
import fr.liglab.lcm.internals.ExplorationStep.Progress;
import fr.liglab.lcm.internals.FrequentsIteratorRenamer;
import fr.liglab.lcm.io.FileCollector;
import fr.liglab.lcm.io.NullCollector;
import fr.liglab.lcm.io.PatternSortCollector;
import fr.liglab.lcm.io.PatternsCollector;
import fr.liglab.lcm.io.PerItemTopKCollector;
import fr.liglab.lcm.io.StdOutCollector;

/**
 * LCM implementation, based on UnoAUA04 :
 * "An Efficient Algorithm for Enumerating Closed Patterns in Transaction Databases"
 * by Takeaki Uno el. al.
 */
public class PLCM {
	final List<PLCMThread> threads;
	private WatcherThread progressWatch;

	private final PatternsCollector collector;

	private final long[] globalCounters;

	public PLCM(PatternsCollector patternsCollector) {
		this(patternsCollector, Runtime.getRuntime().availableProcessors());
	}

	public PLCM(PatternsCollector patternsCollector, int nbThreads) {
		if (nbThreads < 1) {
			throw new IllegalArgumentException("nbThreads has to be > 0, given " + nbThreads);
		}
		this.collector = patternsCollector;
		this.threads = new ArrayList<PLCMThread>(nbThreads);
		this.createThreads(nbThreads);
		this.globalCounters = new long[PLCMCounters.values().length];
	}

	void createThreads(int nbThreads) {
		for (int i = 0; i < nbThreads; i++) {
			this.threads.add(new PLCMThread(i));
		}
	}

	public final void collect(int support, int[] pattern) {
		this.collector.collect(support, pattern);
	}

	void initializeAndStartThreads(final ExplorationStep initState) {
		for (PLCMThread t : this.threads) {
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

		this.progressWatch = new WatcherThread(initState);
		this.progressWatch.start();

		for (PLCMThread t : this.threads) {
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

		PLCMCounters[] counters = PLCMCounters.values();

		for (int i = 0; i < this.globalCounters.length; i++) {
			PLCMCounters counter = counters[i];

			builder.append(", ");
			builder.append(counter.toString());
			builder.append(':');
			builder.append(this.globalCounters[i]);
		}

		return builder.toString();
	}

	ExplorationStep stealJob(PLCMThread thief) {
		// here we need to readlock because the owner thread can write
		for (PLCMThread victim : this.threads) {
			if (victim != thief) {
				ExplorationStep e = stealJob(thief, victim);
				if (e != null) {
					return e;
				}
			}
		}
		return null;
	}

	ExplorationStep stealJob(PLCMThread thief, PLCMThread victim) {
		victim.lock.readLock().lock();
		for (int stealPos = 0; stealPos < victim.stackedJobs.size(); stealPos++) {
			ExplorationStep sj = victim.stackedJobs.get(stealPos);
			ExplorationStep next = sj.next();

			if (next != null) {
				// System.out.println(thief.getName() +
				// " stealing from " + victim.getName());
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
	public enum PLCMCounters {
		ExplorationStepInstances, ExplorationStepCatchedWrongFirstParents, FirstParentTestRejections, TopKRejections, TransactionsCompressions
	}

	public class PLCMThread extends Thread {
		public final long[] counters;
		final ReadWriteLock lock;
		final List<ExplorationStep> stackedJobs;
		private final int id;

		public PLCMThread(final int id) {
			super("PLCMThread" + id);
			this.stackedJobs = new ArrayList<ExplorationStep>();
			this.id = id;
			this.lock = new ReentrantReadWriteLock();
			this.counters = new long[PLCMCounters.values().length];
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
			// no need to readlock, this thread is the only one that can do
			// writes
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
						this.counters[PLCMCounters.ExplorationStepInstances.ordinal()]++;
						this.counters[PLCMCounters.ExplorationStepCatchedWrongFirstParents.ordinal()] += sj
								.getCatchedWrongFirstParentCount();

						this.lock.writeLock().unlock();
					} else {
						this.lcm(extended);
					}

				} else { // our list was empty, we should steal from another
							// thread
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

	private class WatcherThread extends Thread {
		/**
		 * ping delay, in milliseconds
		 */
		private static final long PRINT_STATUS_EVERY = 5 * 60 * 1000;

		private final ExplorationStep step;

		public WatcherThread(ExplorationStep initState) {
			this.step = initState;
		}

		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(PRINT_STATUS_EVERY);
					Progress progress = this.step.getProgression();
					System.err.format("%1$tY/%1$tm/%1$td %1$tk:%1$tM:%1$tS - root iterator state : %2$d/%3$d\n",
							Calendar.getInstance(), progress.current, progress.last);
				} catch (InterruptedException e) {
					return;
				}
			}
		}
	}

	public static void main(String[] args) {

		Options options = new Options();
		CommandLineParser parser = new PosixParser();

		options.addOption("h", false, "Show help");
		options.addOption(
				"b",
				false,
				"(only for standalone) Benchmark mode : show mining time and drop patterns to oblivion (in which case OUTPUT_PATH is ignored)");
		options.addOption("k", true, "Run in top-k-per-item mode");
		options.addOption("t", true, "How many threads will be launched (defaults to your machine's processors count)");
		options.addOption("c", true,
				"How many sockets will share a copy of the data (triggers thread affinity), defaults to all sockets share, no copy");

		try {
			CommandLine cmd = parser.parse(options, args);

			if (cmd.getArgs().length < 2 || cmd.getArgs().length > 3) {
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
		String footer = "\nFor advanced tuning you may also set properties : " + ExplorationStep.KEY_DENSITY_THRESHOLD
				+ ", " + ExplorationStep.KEY_LONG_TRANSACTIONS_THRESHOLD + ", "
				+ ExplorationStep.KEY_VIEW_SUPPORT_THRESHOLD;

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(80, syntax, header, options, footer);
	}

	public static void standalone(CommandLine cmd) {
		String[] args = cmd.getArgs();
		int minsup = Integer.parseInt(args[1]);

		long time = System.currentTimeMillis();
		ExplorationStep initState = new ExplorationStep(minsup, args[0]);
		time = System.currentTimeMillis() - time;
		System.err.println("Dataset loaded in " + time + "ms");

		String outputPath = null;
		if (args.length >= 3) {
			outputPath = args[2];
		}

		int nbSocketsShareCopy = 0;
		if (cmd.hasOption('c')) {
			nbSocketsShareCopy = Integer.parseInt(cmd.getOptionValue('c'));
			PLCMAffinity.bindMainThread();
		}

		PatternsCollector collector = instanciateCollector(cmd, outputPath, initState);

		PLCM miner;
		if (cmd.hasOption('t')) {
			int nbThreads = Integer.parseInt(cmd.getOptionValue('t'));
			if (nbSocketsShareCopy == 0) {
				miner = new PLCM(collector, nbThreads);
			} else {
				miner = new PLCMAffinity(collector, nbThreads, nbSocketsShareCopy);
			}
		} else {
			if (nbSocketsShareCopy == 0) {
				miner = new PLCM(collector);
			} else {
				miner = new PLCMAffinity(collector, nbSocketsShareCopy, false);
			}
		}

		time = System.currentTimeMillis();
		miner.lcm(initState);
		time = System.currentTimeMillis() - time;

		long outputted = collector.close();

		System.err.println(miner.toString() + " // mined in " + time + "ms // outputted " + outputted + " patterns");
	}

	/**
	 * Parse command-line arguments to instanciate the right collector
	 */
	private static PatternsCollector instanciateCollector(CommandLine cmd, String outputPath, ExplorationStep initState) {

		PatternsCollector collector = null;

		if (cmd.hasOption('b')) { // BENCHMARK MODE !
			collector = new NullCollector();
		} else {
			if (outputPath != null) {
				try {
					collector = new FileCollector(outputPath);
				} catch (IOException e) {
					e.printStackTrace(System.err);
					System.err.println("Aborting mining.");
					System.exit(1);
				}
			} else {
				collector = new StdOutCollector();
			}
			collector = new PatternSortCollector(collector);
		}

		if (cmd.hasOption('k')) {
			int k = Integer.parseInt(cmd.getOptionValue('k'));

			FrequentsIteratorRenamer extensions = new FrequentsIteratorRenamer(
					initState.counters.getExtensionsIterator(), initState.counters.getReverseRenaming());

			PerItemTopKCollector topKcoll = new PerItemTopKCollector(collector, k, initState.counters.nbFrequents,
					extensions);

			initState.appendSelector(topKcoll.asSelector());
			collector = topKcoll;
		}

		return collector;
	}
}
