package fr.liglab.lcm;

import fr.liglab.lcm.internals.Dataset;
import fr.liglab.lcm.io.FileReader;
import fr.liglab.lcm.io.PatternsCollector;
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
		Transactions transactions = FileReader.fromClassicAscii(args[0]);
		long minsup = Long.parseLong(args[1]);
		PatternsCollector collector = new StdOutCollector();
		
		Dataset dataset = new Dataset(minsup, transactions);
		LCM miner = new LCM(minsup, collector);
		miner.lcm(dataset);
		
		collector.close();
	}
}
