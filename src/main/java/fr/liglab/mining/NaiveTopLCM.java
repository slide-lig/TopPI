package fr.liglab.mining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.util.GenericOptionsParser;

import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.internals.FrequentsIterator;
import fr.liglab.mining.io.FileCollector;
import fr.liglab.mining.io.NullCollector;
import fr.liglab.mining.io.PatternSortCollector;
import fr.liglab.mining.io.PatternsCollector;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.io.StdOutCollector;
import fr.liglab.mining.util.MemoryPeakWatcherThread;

public class NaiveTopLCM {
	final List<NaiveTopLCMThread> threads;
	final FrequentsIterator starters;
	final PatternsCollector collector;
	final ReentrantLock collectorLock = new ReentrantLock();
	final ExplorationStep initState;
	
	public NaiveTopLCM(int k, PatternsCollector collector, ExplorationStep initState, int nbThreads) {
		this.starters = initState.counters.getExtensionsIdIterator();
		this.threads = new ArrayList<NaiveTopLCMThread>(nbThreads);
		this.collector = collector;
		this.initState = initState;
		
		int[] reverseRenaming = initState.counters.getReverseRenaming();
		
		for (int i = 0; i < nbThreads; i++) {
			this.threads.add(new NaiveTopLCMThread(i, k, reverseRenaming));
		}
	}
	
	void go() {
		for (NaiveTopLCMThread thread : this.threads) {
			thread.run();
		}
	}
	
	private final class NaiveTopLCMThread extends Thread {
		protected final int id;
		protected final FakeIterator items;
		protected final int k;
		protected TopLCM wrapped;
		
		public NaiveTopLCMThread(final int id, int k, int[] reverseRenaming) {
			super("NaiveTopLCMThread" + id);
			this.id = id;
			this.k = k;
			this.items = new FakeIterator(0, reverseRenaming);
		}
		
		@Override
		public long getId() {
			return id;
		}
		
		@Override
		public void run() {
			for (int item = starters.next(); item >= 0; item = starters.next()) {
				this.items.setItem(item);

				this.wrapped = new TopLCM(null, 1);
				this.wrapped.collector = new PerItemTopKCollector(collector, this.k, 1, this.items);
				
				ExplorationStep projected = initState.project(item);
				if (projected != null) {
					projected.appendSelector(this.wrapped.collector.asSelector());
					this.wrapped.lcm(projected);
					
					collectorLock.lock();
					try {
						this.wrapped.collector.outputAll();
					} finally {
						collectorLock.unlock();
					}
				}
			}
		}
	}
	
	
	
	
	
	/////////////////////////////////////////////////////////////////////////////////////
	
	public static void main(String[] args) throws IOException {
		Options options = new Options();
		CommandLineParser parser = new PosixParser();

		options.addOption(
				"b",
				false,
				"(only for standalone) Benchmark mode : show mining time and drop patterns to oblivion (in which case OUTPUT_PATH is ignored)");
		options.addOption("h", false, "Show help");
		options.addOption("k", true, "The K to top-k-per-item mining");
		options.addOption(
				"m",
				false,
				"(only for standalone) Give highest memory usage after mining (instanciates a watcher thread that periodically triggers garbage collection)");
		options.addOption("r", true, "path to a file giving, per line, ITEM_ID NB_PATTERNS_TO_KEEP");
		options.addOption("s", false, "Sort items in outputted patterns, in ascending order");
		options.addOption("t", true, "How many threads will be launched (defaults to your machine's processors count)");
		options.addOption("u", false, "(only for standalone) output unique patterns only");
		options.addOption("v", false, "Enable verbose mode, which logs every extension of the empty pattern");
		options.addOption("V", false,
				"Enable ultra-verbose mode, which logs every pattern extension (use with care: it may produce a LOT of output)");
		options.addOption("w", true, "Width of the breadth-first exploration - defaults to 0");

		try {
			GenericOptionsParser hadoopCmd = new GenericOptionsParser(args);
			CommandLine cmd = parser.parse(options, hadoopCmd.getRemainingArgs());

			if (cmd.getArgs().length < 2 || cmd.getArgs().length > 3 || cmd.hasOption('h')) {
				printMan(options);
			} else if (!cmd.hasOption('k')) {
				System.err.println("-k parameter is mandatory");
				System.exit(1);
			} else {
				standalone(cmd);
			}
		} catch (ParseException e) {
			printMan(options);
		}
	}
	
	public static void printMan(Options options) {
		String syntax = "java fr.liglab.mining.NaiveTopLCM [OPTIONS] INPUT_PATH MINSUP [OUTPUT_PATH]";
		String header = "\nIf OUTPUT_PATH is missing, patterns are printed to standard output.\nOptions are :";
		String footer = "";
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(80, syntax, header, options, footer);
	}
	
	private static void standalone(CommandLine cmd) {
		String[] args = cmd.getArgs();
		int minsup = Integer.parseInt(args[1]);
		MemoryPeakWatcherThread memoryWatch = null;

		String outputPath = null;
		if (args.length >= 3) {
			outputPath = args[2];
		}
		
		int k = Integer.parseInt(cmd.getOptionValue('k'));
		
		if (cmd.hasOption('m')) {
			memoryWatch = new MemoryPeakWatcherThread();
			memoryWatch.start();
		}

		long chrono = System.currentTimeMillis();
		ExplorationStep initState = new ExplorationStep(minsup, args[0]);
		long loadingTime = System.currentTimeMillis() - chrono;
		System.err.println("Dataset loaded in " + loadingTime + "ms");

		if (cmd.hasOption('V')) {
			ExplorationStep.verbose = true;
			ExplorationStep.ultraVerbose = true;
		} else if (cmd.hasOption('v')) {
			ExplorationStep.verbose = true;
		}
		
		if (cmd.hasOption('w')) {
			ExplorationStep.BREADTH_SIZE = Integer.parseInt(cmd.getOptionValue('w'));
		}

		int nbThreads = Runtime.getRuntime().availableProcessors();
		if (cmd.hasOption('t')) {
			nbThreads = Integer.parseInt(cmd.getOptionValue('t'));
		}
		
		PatternsCollector collector = instanciateCollector(cmd, outputPath);
		chrono = System.currentTimeMillis();
		
		NaiveTopLCM miner = new NaiveTopLCM(k, collector, initState, nbThreads);
		miner.go();
		
		chrono = System.currentTimeMillis() - chrono;

		Map<String, Long> additionalCounters = new HashMap<String, Long>();
		additionalCounters.put("miningTime", chrono);
		additionalCounters.put("outputtedPatterns", collector.close());
		additionalCounters.put("loadingTime", loadingTime);
		additionalCounters.put("avgPatternLength", (long) collector.getAveragePatternLength());

		if (memoryWatch != null) {
			memoryWatch.interrupt();
			additionalCounters.put("maxUsedMemory", memoryWatch.getMaxUsedMemory());
		}

		System.err.println(additionalCounters.toString());
	}
	
	private static PatternsCollector instanciateCollector(CommandLine cmd, String outputPath) {

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

			if (cmd.hasOption('s')) {
				collector = new PatternSortCollector(collector);
			}
		}

		return collector;
	}
	
	private static final class FakeIterator implements FrequentsIterator {
		
		private boolean iterated = false;
		private int item;
		private int[] renaming;
		
		FakeIterator(int singleItem, int[] reverseRenaming) {
			this.item = singleItem;
			this.renaming = reverseRenaming;
		}
		
		void setItem(int i) {
			this.item = i;
			this.iterated = false;
		}
		
		@Override
		public int next() {
			if (this.iterated){
				return -1; 
			} else {
				this.iterated = true;
				return this.renaming[this.item];
			}
		}

		@Override
		public int peek() {
			return this.renaming[this.item];
		}

		@Override
		public int last() {
			return this.renaming[this.item];
		}
		
	}

}
