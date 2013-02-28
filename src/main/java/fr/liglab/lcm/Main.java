package fr.liglab.lcm;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import fr.liglab.lcm.internals.RebasedConcatenatedDataset;
import fr.liglab.lcm.internals.RebasedDataset;
import fr.liglab.lcm.io.FileCollector;
import fr.liglab.lcm.io.FileReader;
import fr.liglab.lcm.io.NullCollector;
import fr.liglab.lcm.io.PatternsCollector;
import fr.liglab.lcm.io.PerItemTopKCollector;
import fr.liglab.lcm.io.RebaserCollector;
import fr.liglab.lcm.io.StdOutCollector;

/**
 * Program entry
 * @author Martin Kirchgessner
 */
public class Main {
	public static void main(String[] args) {
		
		Options options = new Options();
		CommandLineParser parser = new PosixParser();
		
		options.addOption("h", false, "Show help");
		options.addOption("b", false, "(only for standalone) Benchmark mode : show mining time and drop patterns to oblivion (in which case OUTPUT_PATH is ignored)");
		options.addOption("k", true, "Run in top-k-per-item mode");
		
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

		RebasedConcatenatedDataset dataset = new RebasedConcatenatedDataset(minsup, reader);
		
		String outputPath = null;
		if (args.length >= 3) {
			outputPath = args[2];
		}
		
		PatternsCollector collector = instanciateCollector(cmd, outputPath, dataset);
		
		long time = System.currentTimeMillis();
		
		LCM miner = new LCM(collector);
		miner.lcm(dataset);

		if (cmd.hasOption('b')) {
			time = System.currentTimeMillis() - time;
			System.err.println(miner.toString() + " // mined in " + time + "ms");
		}
		
		reader.close();
		collector.close();
	}
	
	/**
	 * Parse command-line arguments to instanciate the right collector
	 * in stand-alone mode we're always rebasing so we need the dataset
	 */
	private static PatternsCollector instanciateCollector(CommandLine cmd, 
			String outputPath, RebasedDataset dataset) {
		
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

			collector = new RebaserCollector(collector, dataset);
		}
		
		if (cmd.hasOption('k')) {
			int k = Integer.parseInt(cmd.getOptionValue('k'));
			collector = new PerItemTopKCollector(collector, k, true);
		}
		
		return collector;
	}
}
