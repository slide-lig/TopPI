package fr.liglab.lcm;

import java.io.IOException;

import fr.liglab.lcm.internals.RebasedConcatenatedDataset;
import fr.liglab.lcm.io.FileCollector;
import fr.liglab.lcm.io.FileReader;
import fr.liglab.lcm.io.PatternsCollector;
import fr.liglab.lcm.io.RebaserCollector;
import fr.liglab.lcm.io.StdOutCollector;

/**
 * Program entry
 * @author Martin Kirchgessner
 */
public class Main {
	public static void main(String[] args) {
		if (args.length < 2 || args.length > 3) {
			printMan();
		} else {
			standalone(args);
		}
	}
	
	public static void printMan() {
		System.out.println("USAGE :");
		System.out.println("\tjava fr.liglab.LCM INPUT_PATH MINSUP [OUTPUT_PATH]\n");
		System.out.println("If OUTPUT_PATH is missing, patterns are printed to standard output");
	}
	
	public static void standalone(String[] args) {
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
		
		LCM miner = new LCM(collector);
		miner.lcm(dataset);
		
		reader.close();
		collector.close();
	}
}
