package fr.liglab.lcm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.internals.DatasetCounters;
import fr.liglab.lcm.internals.DatasetCounters.FrequentsIterator;
import fr.liglab.lcm.internals.DatasetFactory;
import fr.liglab.lcm.internals.DatasetFactory.DontExploreThisBranchException;
import fr.liglab.lcm.internals.DatasetRebaserCounters;
import fr.liglab.lcm.io.FileCollector;
import fr.liglab.lcm.io.NullCollector;
import fr.liglab.lcm.io.PatternSortCollector;
import fr.liglab.lcm.io.PatternsCollector;
import fr.liglab.lcm.io.PerItemTopKCollectorThreadSafeInitialized;
import fr.liglab.lcm.io.RebaserCollector;
import fr.liglab.lcm.io.StdOutCollector;
import fr.liglab.lcm.util.ItemsetsFactory;

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
	private final AtomicInteger pptestFailed = new AtomicInteger(0);

	public PLCM(PatternsCollector patternsCollector) {
		this(patternsCollector, Runtime.getRuntime().availableProcessors());
	}

	public PLCM(PatternsCollector patternsCollector, int nbThreads) {
		if (nbThreads < 1) {
			throw new IllegalArgumentException("nbThreads has to be > 0, given " + nbThreads);
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

	public final int explore(StackedJob sj, int extension) {
		int previousItem = -1;
		int previousResult = -1;
		synchronized (sj) {
			previousItem = sj.getPreviousItem();
			previousResult = sj.getPreviousResult();
		}
		synchronized (sj.failedpptests) {
			return this.collector.explore(sj.pattern, extension, sj.sortedfreqs, 
					sj.dataset.getCounters().supportCounts, sj.failedpptests, previousItem, previousResult);
		}
	}

	/**
	 * Initial invocation
	 */
	public void lcm(final Dataset dataset) {
		final DatasetCounters counters = dataset.getCounters();
		
		final int[] pattern = counters.closure;
		
		if (pattern.length > 0) {
			collector.collect(counters.transactionsCount, pattern);
		}

		this.threads.get(0).init(dataset, pattern);
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
		return "LCM exploration : " + explored + " patterns explored / " + cut + " aborted / " + pptestFailed
				+ " pptest failed";
	}

	public StolenJob stealJob(final int id) {
		// here we need to readlock because the owner thread can write
		for (int stealFrom = 0; stealFrom < this.threads.size(); stealFrom++) {
			if (stealFrom != id) {
				PLCMThread t = this.threads.get(stealFrom);
				for (int stealPos = 0; stealPos < t.stackedJobs.size(); stealPos++) {
					t.lock.readLock().lock();
					if (!t.stackedJobs.isEmpty()) {
						StackedJob sj = t.stackedJobs.get(0);
						t.lock.readLock().unlock();
						
						int extension = sj.iterator.next();
						
						if (extension != -1) {
							// need to copy because of possible inconsistencies
							// in previous explore results (no lock to
							// read/update them)
							return new StolenJob(extension, sj);
						}
					} else {
						t.lock.readLock().unlock();
					}
				}
			}
		}
		return null;
	}
	
	/**
	 * Don't use it on dense datasets. Just don't.
	 */
	@Deprecated
	public void setUltraVerboseMode(boolean enabled) {
		for (PLCMThread t : this.threads) {
			t.setUltraVerboseMode(enabled);
		}
	}

	private class PLCMThread extends Thread {
		private final ReadWriteLock lock;
		private final List<StackedJob> stackedJobs;
		private final int id;
		private boolean ultraVerbose = false;

		public PLCMThread(final int id) {
			super("PLCMThread" + id);
			this.stackedJobs = new ArrayList<StackedJob>();
			this.id = id;
			this.lock = new ReentrantReadWriteLock();
		}

		public void setUltraVerboseMode(boolean enabled) {
			this.ultraVerbose = enabled;
		}

		private void init(Dataset dataset, int[] pattern) {
			FrequentsIterator it = dataset.getCounters().getFrequentsIterator();
			StackedJob sj = new StackedJob(dataset, pattern, it);
			this.lock.writeLock().lock();
			this.stackedJobs.add(sj);
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
				StackedJob sj = null;
				int extension = -1;
				if (!this.stackedJobs.isEmpty()) {
					sj = this.stackedJobs.get(this.stackedJobs.size() - 1);
				}
				if (sj != null) {
					extension = sj.iterator.next();
					// iterator is finished, remove it from the stack
					if (extension == -1) {
						this.lock.writeLock().lock();
						this.stackedJobs.remove(this.stackedJobs.size() - 1);
						this.lock.writeLock().unlock();
					} else {
						this.lcm(sj, extension);
					}
					
				} else { // our list was empty, we should steal from another thread
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
			int explore = explore(sj, extension);
			
			if (explore >= 0) {
				sj.updateExploreResults(extension, explore);
				cut.incrementAndGet();
				return;
			}
			
			Dataset dataset = null;
			try {
				dataset = DatasetFactory.project(sj.dataset, extension);
			} catch (DontExploreThisBranchException e) {
				sj.updatepptestfail(e.extension, e.firstParent);
				pptestFailed.incrementAndGet();
				return;
			}
			
			explored.incrementAndGet();

			if (this.ultraVerbose) {
				System.out
						.format("%1$tY/%1$tm/%1$td %1$tk:%1$tM:%1$tS - thread %2$d exploring %3$s (%4$d transactions in DB) with %5$d\n",
								Calendar.getInstance(), this.id, Arrays.toString(sj.pattern), 
								sj.dataset.getCounters().transactionsCount, extension);
			}
			
			DatasetCounters counters = dataset.getCounters();
			
			int[] Q = ItemsetsFactory.extend(counters.closure, extension, sj.pattern);
			collect(counters.transactionsCount, Q);
			
			StackedJob nj = new StackedJob(dataset, Q, counters.getFrequentsIteratorTo(extension));
			this.lock.writeLock().lock();
			this.stackedJobs.add(nj);
			this.lock.writeLock().unlock();
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
		options.addOption("b", false, "(only for standalone) Benchmark mode : show mining time and drop patterns to oblivion (in which case OUTPUT_PATH is ignored)");
		options.addOption("k", true, "Run in top-k-per-item mode");
		options.addOption("t", true, "How many threads will be launched (defaults to your machine's processors count)");

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
		int minsup = Integer.parseInt(args[1]);
		
		Dataset dataset = DatasetFactory.fromFile(minsup, args[0]);
		
		String outputPath = null;
		if (args.length >= 3) {
			outputPath = args[2];
		}

		PatternsCollector collector = instanciateCollector(cmd, outputPath, dataset);

		long time = System.currentTimeMillis();
		
		PLCM miner;
		if (cmd.hasOption('t')) {
			int nbThreads = Integer.parseInt(cmd.getOptionValue('t'));
			miner = new PLCM(collector, nbThreads);
		} else {
			miner = new PLCM(collector);
		}

		miner.lcm(dataset);

		if (cmd.hasOption('b')) {
			time = System.currentTimeMillis() - time;
			System.err.println(miner.toString() + " // mined in " + time + "ms");
		}
		
		collector.close();
	}

	/**
	 * Parse command-line arguments to instanciate the right collector
	 */
	private static PatternsCollector instanciateCollector(CommandLine cmd, String outputPath, Dataset dataset) {

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
			
			if (dataset.getCounters() instanceof DatasetRebaserCounters) {
				collector = new RebaserCollector(collector, (DatasetRebaserCounters) dataset.getCounters());
			}
		}
		
		if (cmd.hasOption('k')) {
			int k = Integer.parseInt(cmd.getOptionValue('k'));
			collector = new PerItemTopKCollectorThreadSafeInitialized(collector, k, dataset, true);
		}

		return collector;
	}
}
