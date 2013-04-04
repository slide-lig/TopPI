package fr.liglab.lcm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import fr.liglab.lcm.LCM.DontExploreThisBranchException;
import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.ExtensionsIterator;
import fr.liglab.lcm.internals.RebasedConcatenatedDataset;
import fr.liglab.lcm.internals.RebasedDataset;
import fr.liglab.lcm.io.FileCollector;
import fr.liglab.lcm.io.FileReader;
import fr.liglab.lcm.io.NullCollector;
import fr.liglab.lcm.io.PatternsCollector;
import fr.liglab.lcm.io.PerItemTopKCollectorThreadSafeInitialized;
import fr.liglab.lcm.io.RebaserCollector;
import fr.liglab.lcm.io.StdOutCollector;
import fr.liglab.lcm.util.ItemsetsFactory;
import gnu.trove.map.TIntIntMap;

/**
 * LCM implementation, based on UnoAUA04 :
 * "An Efficient Algorithm for Enumerating Closed Patterns in Transaction Databases"
 * by Takeaki Uno el. al.
 */
public class PLCM {
	private final List<PLCMThread> threads;

	private final PatternsCollector collector;

	private final AtomicInteger explored = new AtomicInteger(0);
	private final AtomicInteger cut = new AtomicInteger(0);
	private final Random stealRandom = new Random();

	public PLCM(PatternsCollector patternsCollector) {
		this(patternsCollector, Runtime.getRuntime().availableProcessors());
	}

	public PLCM(PatternsCollector patternsCollector, int nbThreads) {
		if (nbThreads < 1) {
			throw new IllegalArgumentException(
					"nbThreads has to be > 0, given " + nbThreads);
		}
		collector = patternsCollector;
		this.threads = new ArrayList<PLCMThread>(nbThreads);
		for (int i = 0; i < nbThreads; i++) {
			this.threads.add(new PLCMThread(i));
		}
	}

	public final void collect(int support, int[] pattern) {
		this.collector.collect(support, pattern);
	}

	public final int explore(int[] currentPattern, int extension,
			int[] sortedFreqItems, TIntIntMap supportCounts, int previousItem,
			int previousResult) {
		return this.collector.explore(currentPattern, extension,
				sortedFreqItems, supportCounts, null, previousItem, previousResult);
	}

	/**
	 * Initial invocation
	 */
	public void lcm(final Dataset dataset) {
		int[] pattern = dataset.getDiscoveredClosureItems(); // usually, it's
																// empty

		if (pattern.length > 0) {
			collector.collect(dataset.getTransactionsCount(), pattern);
		}

		ExtensionsIterator iterator = dataset.getCandidatesIterator();
		this.threads.get(0).init(iterator, dataset, pattern);
		for (PLCMThread t : this.threads) {
			// System.out.println("Starting thread " + t.id);
			t.start();
		}
		for (PLCMThread t : this.threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public String toString() {
		return "LCM exploration : " + explored + " patterns explored / " + cut
				+ " aborted";
	}

	public StolenJob stealJob(final int id) {
		// here we need to readlock because the owner thread can write
		int startSteal = this.stealRandom.nextInt(this.threads.size());
		for (int stealDelta = 0; stealDelta < this.threads.size(); stealDelta++) {
			int stealFrom = (startSteal + stealDelta) % this.threads.size();
			if (stealFrom != id) {
				PLCMThread t = this.threads.get(stealFrom);
				for (int stealPos = 0; stealPos < t.stackedJobs.size(); stealPos++) {
					t.lock.readLock().lock();
					if (!t.stackedJobs.isEmpty()) {
						StackedJob sj = t.stackedJobs.get(0);
						t.lock.readLock().unlock();
						int extension = sj.iterator.getExtension();
						if (extension != -1) {
							// need to copy because of possible inconsistencies
							// in previous explore results (no lock to
							// read/update them)
							return new StolenJob(extension, new StackedJob(
									sj.iterator, sj.dataset, sj.pattern,
									sj.sortedfreqs));
						}
					} else {
						t.lock.readLock().unlock();
					}
				}
			}
		}
		return null;
	}

	private class PLCMThread extends Thread {
		private final ReadWriteLock lock;
		private final List<StackedJob> stackedJobs;
		private final int id;

		public PLCMThread(final int id) {
			super("PLCMThread" + id);
			this.stackedJobs = new ArrayList<StackedJob>();
			this.id = id;
			this.lock = new ReentrantReadWriteLock();
		}

		private void init(ExtensionsIterator iterator, Dataset dataset,
				int[] pattern) {
			StackedJob sj = new StackedJob(iterator, dataset, pattern, null);
			this.lock.writeLock().lock();
			this.stackedJobs.add(sj);
			this.lock.writeLock().unlock();
		}

		@Override
		public void run() {
			// no need to readlock, this thread is the only one that can do
			// writes
			boolean exit = false;
			while (!exit) {
				StackedJob sj = null;
				int extension = -1;
				if (!this.stackedJobs.isEmpty()) {
					sj = this.stackedJobs.get(this.stackedJobs.size() - 1);
				}
				if (sj != null) {
					extension = sj.iterator.getExtension();
					// iterator is finished, remove it from the stack
					if (extension == -1) {
						this.lock.writeLock().lock();
						this.stackedJobs.remove(this.stackedJobs.size() - 1);
						this.lock.writeLock().unlock();
					}
				}
				if (extension != -1) {
					this.lcm(sj, extension);
				}
				// our list was empty, we should steal from another thread
				if (sj == null) {
					StolenJob stolj = stealJob(this.id);
					if (stolj == null) {
						exit = true;
					} else {
						lcm(stolj.stolenJob, stolj.extension);
					}
				}
			}
		}

		private void lcm(StackedJob sj, int extension) {
			TIntIntMap supportCounts = sj.dataset.getSupportCounts();
			int explore;
			if (sj.sortedfreqs == null) {
				explore = -1;
			} else {
				explore = explore(sj.pattern, extension, sj.sortedfreqs,
						supportCounts, sj.previousItem, sj.previousResult);
			}
			if (explore < 0) {
				explored.incrementAndGet();
				try {
					Dataset dataset = sj.dataset.getProjection(extension);
					int[] Q = ItemsetsFactory.extend(sj.pattern, extension,
							dataset.getDiscoveredClosureItems());
					collect(dataset.getTransactionsCount(), Q);
					ExtensionsIterator iterator = dataset.getCandidatesIterator();
					int[] sortedFreqs = iterator.getSortedFrequents();
					StackedJob nj = new StackedJob(iterator, dataset, Q,
							sortedFreqs);
					this.lock.writeLock().lock();
					this.stackedJobs.add(nj);
					this.lock.writeLock().unlock();
				} catch (DontExploreThisBranchException e) {
					// may happen in getProjection, in which case we should just continue with next candidate
				}
				
			} else {
				sj.previousItem = extension;
				sj.previousResult = explore;
				cut.incrementAndGet();
			}
		}
	}

	private static final class StackedJob {
		private final ExtensionsIterator iterator;
		private final Dataset dataset;
		private final int[] pattern;
		private final int[] sortedfreqs;
		private int previousItem;
		private int previousResult;

		public StackedJob(ExtensionsIterator iterator, Dataset dataset,
				int[] pattern, int[] sortedfreqs) {
			super();
			this.iterator = iterator;
			this.dataset = dataset;
			this.pattern = pattern;
			this.sortedfreqs = sortedfreqs;
			this.previousItem = -1;
			this.previousResult = -1;
		}

		@Override
		public String toString() {
			return "StackedJob [pattern=" + Arrays.toString(pattern) + "]";
		}

	}

	private static final class StolenJob {
		private final int extension;
		private final StackedJob stolenJob;

		public StolenJob(int extension, StackedJob sj) {
			super();
			this.extension = extension;
			this.stolenJob = sj;
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
		options.addOption("t", true,
				"How many threads will be launched (defaults to 8)");

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
		String footer = "\nBy martin.kirchgessner@imag.fr";

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(80, syntax, header, options, footer);
	}

	public static void standalone(CommandLine cmd) {
		String[] args = cmd.getArgs();

		FileReader reader = new FileReader(args[0]);
		int minsup = Integer.parseInt(args[1]);

		RebasedConcatenatedDataset dataset = null;
		try {
			dataset = new RebasedConcatenatedDataset(minsup, reader);
		} catch (DontExploreThisBranchException e) {
			// this won't ever happen with initial constructor
			e.printStackTrace();
		}

		String outputPath = null;
		if (args.length >= 3) {
			outputPath = args[2];
		}

		PatternsCollector collector = instanciateCollector(cmd, outputPath,
				dataset, dataset);

		long time = System.currentTimeMillis();

		int nbThreads = 8;
		if (cmd.hasOption('t')) {
			nbThreads = Integer.parseInt(cmd.getOptionValue('t'));
		}

		PLCM miner = new PLCM(collector, nbThreads);
		miner.lcm(dataset);

		if (cmd.hasOption('b')) {
			time = System.currentTimeMillis() - time;
			System.err
					.println(miner.toString() + " // mined in " + time + "ms");
		}

		reader.close();
		collector.close();
	}

	/**
	 * Parse command-line arguments to instanciate the right collector in
	 * stand-alone mode we're always rebasing so we need the dataset
	 */
	private static PatternsCollector instanciateCollector(CommandLine cmd,
			String outputPath, RebasedDataset rebasedDataset, Dataset dataset) {

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

			collector = new RebaserCollector(collector, rebasedDataset);
		}

		if (cmd.hasOption('k')) {
			int k = Integer.parseInt(cmd.getOptionValue('k'));
			collector = new PerItemTopKCollectorThreadSafeInitialized(
					collector, k, dataset, true);
		}

		return collector;
	}
}
