package fr.liglab.mining;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.ws.Holder;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import fr.liglab.mining.internals.ExplorationStep;
import fr.liglab.mining.io.FileCollector;
import fr.liglab.mining.io.FileCollectorWithIDMapper;
import fr.liglab.mining.io.NullCollector;
import fr.liglab.mining.io.PatternSortCollector;
import fr.liglab.mining.io.PatternsCollector;
import fr.liglab.mining.io.PerItemTopKCollector;
import fr.liglab.mining.io.PerItemTopKtoJSONCollector;
import fr.liglab.mining.io.StdOutCollector;
import fr.liglab.mining.mapred.TopPIoverHadoop;
import fr.liglab.mining.util.MemoryPeakWatcherThread;

public class TopPIcli {
	protected static long chrono;
	
	public static Options getOptions() {
		Options options = new Options();

		options.addOption(
				"b",
				false,
				"(only for standalone) Benchmark mode : show mining time and drop patterns to oblivion (in which case OUTPUT_PATH is ignored)");
		options.addOption("c", true, "over-filter to get top-c-correlated per-item patterns");
		// FIXME
		//options.addOption("B", false, "Do a 3-passes preliminary jobs - an experiment for datasets with more than 2 million items");
		options.addOption("e", false, "DEBUG ONLY - prints to stdout the raised threshold, for each starter item");
		options.addOption("g", true,
				"Enables Hadoop and gives the number of groups in which the search space will be splitted");
		options.addOption("h", false, "Show help");
		options.addOption(
				"i",
				false,
				"(only for standalone) Outputs a single pattern for each frequent item. "
						+ "Given support is item's support count and pattern's items are "
						+ "the item itself, its patterns count (max=K), its patterns' supports sum and its lowest pattern support.");
		options.addOption("J", false, "(implies -S) outputs per-item top-K itemsets to standard output as JSON");
		options.addOption("k", true, "The 'K' in top-K-per-item mining");
		options.addOption(
				"m",
				false,
				"(only for standalone) Give highest memory usage after mining (instanciates a watcher thread that periodically triggers garbage collection)");
		options.addOption(
				"p",
				true,
				"Comma-separated frequency thresholds that should be used for pre-filtered datasets. Warning: we create a thread for each.");
		options.addOption("r", true, "path to a file giving, per line, ITEM_ID NB_PATTERNS_TO_KEEP");
		options.addOption("s", false, "(only for standalone) Sort items in outputted patterns, in ascending order");
		options.addOption("S", false, "(only for standalone) enable arbitrary strings as item IDs in the input file");
		options.addOption("t", true, "How many threads will be launched (defaults to your machine's processors count)");
		options.addOption("u", false, "(only for standalone) output unique patterns only");
		options.addOption("v", false, "Enable verbose mode, which logs every extension of the empty pattern");
		options.addOption("V", false,
				"Enable ultra-verbose mode, which logs every pattern extension (use with care: it may produce a LOT of output)");
		
		return options;
	}
	
	public static void main(String[] args) throws Exception {
		Options options = getOptions();
		
		try {
			CommandLineParser parser = new PosixParser();
			GenericOptionsParser hadoopCmd = new GenericOptionsParser(args);
			CommandLine cmd = parser.parse(options, hadoopCmd.getRemainingArgs());

			if (cmd.getArgs().length < 2 || cmd.getArgs().length > 3 || cmd.hasOption('h')) {
				printMan(options);
			} else if (!cmd.hasOption('k')) {
				System.err.println("-k parameter is mandatory");
				System.exit(1);
			} else if (cmd.hasOption('g')) {
				hadoop(args);
			} else {
				standalone(cmd);
			}
		} catch (ParseException e) {
			printMan(options);
		}
	}

	public static void printMan(Options options) {
		String syntax = "java fr.liglab.mining.TopPIcli -k [K] [OPTIONS] INPUT_PATH MINSUP [OUTPUT_PATH]";
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
		if (cmd.hasOption('S') || cmd.hasOption('J')) {
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

		TopPI miner = new TopPI(collector, nbThreads, true);
		miner.startMining(initState);
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
		
		Map<Integer, String> itemIDmap = null;
		if (itemIDmapHolder != null) {
			itemIDmap = new HashMap<Integer, String>(itemIDmapHolder.value.size());
			for (Entry<String, Integer> entry : itemIDmapHolder.value.entrySet()) {
				itemIDmap.put(entry.getValue(), entry.getKey());
			}
		}
		
		if (cmd.hasOption('b')) { // BENCHMARK MODE !
			collector = new NullCollector();
		} else {
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

		if (cmd.hasOption('J')) {
			topKcoll = new PerItemTopKtoJSONCollector(k, initState, itemIDmap);
		} else {
			topKcoll = new PerItemTopKCollector(collector, k, initState);
		}
		
		topKcoll.setInfoMode(cmd.hasOption('i'));
		topKcoll.setOutputUniqueOnly(cmd.hasOption('u'));

		if (cmd.hasOption('r')) {
			topKcoll.readPerItemKFrom(cmd.getOptionValue('r'));
		}

		return topKcoll;
	}

	public static void hadoop(String[] args) throws Exception {
		System.exit(ToolRunner.run(new TopPIoverHadoop(), args));
	}

}
