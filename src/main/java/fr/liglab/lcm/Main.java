package fr.liglab.lcm;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import fr.liglab.lcm.internals.RebasedConcatenatedDataset;
import fr.liglab.lcm.io.FileCollector;
import fr.liglab.lcm.io.FileReader;
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
		formatter.printHelp(syntax, header, options, footer);
	}
	
	public static void standalone(CommandLine cmd) {
		String[] args = cmd.getArgs();
		
		FileReader reader = new FileReader(args[0]);
		int minsup = Integer.parseInt(args[1]);
		
		PatternsCollector collector = null;
		
		if (args.length >= 3) {
			try {
				collector = new FileCollector(args[2]);
			} catch (IOException e) {
				e.printStackTrace(System.err);
				System.err.println("Aborting mining.");
				System.exit(1);
			}
		} else {
			collector = new StdOutCollector();
		}

		RebasedConcatenatedDataset dataset = new RebasedConcatenatedDataset(minsup, reader);
		collector = new RebaserCollector(collector, dataset);
		
		if (cmd.hasOption('k')) {
			int k = Integer.parseInt(cmd.getOptionValue('k'));
			collector = new PerItemTopKCollector(collector, k, true);
		}
		
		LCM miner = new LCM(collector);
		miner.lcm(dataset);
		
		reader.close();
		collector.close();
		
		System.err.println(miner.toString());
	}
}
