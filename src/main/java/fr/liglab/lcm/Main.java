package fr.liglab.lcm;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import fr.liglab.lcm.internals.RebasedConcatenatedDataset;
import fr.liglab.lcm.internals.RebasedDataset;
import fr.liglab.lcm.io.FileCollector;
import fr.liglab.lcm.io.FileReader;
import fr.liglab.lcm.io.NullCollector;
import fr.liglab.lcm.io.PatternsCollector;
import fr.liglab.lcm.io.PerItemTopKCollector;
import fr.liglab.lcm.io.RebaserCollector;
import fr.liglab.lcm.io.StdOutCollector;
import fr.liglab.lcm.mapred.Driver;

/**
 * Program entry
 * @author Martin Kirchgessner
 */
public class Main {
	
	private static final String OPTION_BENCHMARK = "b";
	private static final String OPTION_TOPK = "k";
	private static final String OPTION_STANDALONE = "a";
	private static final String OPTION_GROUPS = "g";
	
	public static Options getOptions() {
		Options options = new Options();
		
		options.addOption("h", false, "Show help");
		options.addOption(Main.OPTION_STANDALONE, "alone", false, 
				"Run in standalone mode (otherwise runs in map-reduce jobs)");
		options.addOption(Main.OPTION_BENCHMARK, false, 
				"(only for standalone) Benchmark mode : show mining time and drop patterns to oblivion (in which case OUTPUT_PATH is ignored)");
		options.addOption(Main.OPTION_GROUPS, true,
				"(only for map-reduce) Number of groups in which frequent items are dispatched (defaults to 50)");
		options.addOption(Main.OPTION_TOPK, true, 
				"Run in top-k-per-item mode");
		
		return options;
	}
	
	public static void main(String[] args) {
		
		Options options = getOptions();
		CommandLineParser parser = new PosixParser();
		
		try {
			Configuration conf = new Configuration();
			GenericOptionsParser hadoopCmd = new GenericOptionsParser(conf, args);
			CommandLine cmd = parser.parse(options, hadoopCmd.getRemainingArgs());
			String[] remainingArgs = cmd.getArgs();
			
			if (remainingArgs.length == 3 || 
					(remainingArgs.length ==2 && cmd.hasOption(OPTION_STANDALONE)) ) {
				
				String input = remainingArgs[0];
				int minSupport = Integer.parseInt(remainingArgs[1]);
				
				String output = null;
				if (remainingArgs.length == 3) {
					output = remainingArgs[2];
				}
				
				Integer k = null;
				if (cmd.hasOption(OPTION_TOPK)) {
					k = Integer.parseInt(cmd.getOptionValue(OPTION_TOPK));
					
					if (k <= 0) {
						throw new RuntimeException("When provided, K must be positive");
					}
				}
				
				if (cmd.hasOption(OPTION_STANDALONE)) {
					boolean benchMode = cmd.hasOption(OPTION_BENCHMARK);
					standalone(input, minSupport, output, k, benchMode);
					
				} else {
					Integer g = 50;
					if (cmd.hasOption(OPTION_GROUPS)) {
						g = Integer.parseInt(cmd.getOptionValue(OPTION_GROUPS));
						
						if (g <= 0) {
							throw new RuntimeException("When provided, G must be positive");
						}
					}
					
					conf = hadoopCmd.getConfiguration();
					
					conf.setStrings(Driver.KEY_INPUT, input);
					conf.setStrings(Driver.KEY_OUTPUT, output);
					conf.setInt(Driver.KEY_MINSUP, minSupport);
					conf.setInt(Driver.KEY_NBGROUPS, g);
					
					if (k != null) {
						conf.setInt(Driver.KEY_DO_TOP_K, k);
					}
					
					Driver driver = new Driver(conf);
					System.exit(driver.run());
				}
			} else {
				printMan(options);
			}
		} catch (ParseException e) {
			System.out.println(Arrays.toString(args));
			printMan(options);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void printMan(Options options) {
		String syntax = "hadoop jar [path/to/lcm.jar] [GENERIC_OPTIONS] [OPTIONS] INPUT_PATH MINSUP OUTPUT_PATH";
		String header = "\nOUTPUT_PATH is optional in standalone mode. When missing, patterns are printed to standard output.\n\nOptions are :";
		String footer = "\n\nReal hackers play with "+Driver.KEY_GROUPER_CLASS;
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(syntax, header, options, footer);
		
		GenericOptionsParser.printGenericCommandUsage(System.out);
	}
	
	/**
	 * @param input
	 * @param minSupport
	 * @param output may be null
	 * @param k may be null
	 * @param benchMode
	 */
	public static void standalone(String input, int minSupport, String output,
			Integer k, boolean benchMode) {
		
		FileReader reader = new FileReader(input);
		RebasedConcatenatedDataset dataset = new RebasedConcatenatedDataset(minSupport, reader);
		
		PatternsCollector collector = instanciateCollector(output, k, benchMode, dataset);
		
		long time = System.currentTimeMillis();
		
		LCM miner = new LCM(collector);
		miner.lcm(dataset);
		
		time = System.currentTimeMillis() - time;

		reader.close();
		long collected = collector.close();
		
		if (benchMode) {	
			System.err.println(miner.toString() + " // mined in " + time + "ms // " + collected + " patterns outputted");
		}
		
	}
	
	/**
	 * Parse command-line arguments to instanciate the right collector
	 * in standalone mode we're always rebasing so we need the dataset
	 */
	private static PatternsCollector instanciateCollector(String output,
			Integer k, boolean benchMode, RebasedDataset dataset) {
		
		PatternsCollector collector = null;
		
		if (benchMode) {	
			collector = new NullCollector();
		} else {
			if (output != null) {
				try {
					collector = new FileCollector(output);
				} catch (IOException e) {
					e.printStackTrace(System.err);
					System.err.println("Aborting mining.");
					System.exit(1);
				}
			} else {
				collector = new StdOutCollector();
			}

			collector = new RebaserCollector(collector, dataset);
		}
		
		if (k != null) {
			collector = new PerItemTopKCollector(collector, k, false);
		}
		
		return collector;
	}
}
