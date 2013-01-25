package fr.liglab;

import gnu.trove.map.hash.TIntLongHashMap;
import gnu.trove.procedure.TIntProcedure;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

public class LCM {
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
		ArrayList<TIntSet> dataset = readFile(args[0]);
		int minsup = Integer.parseInt(args[1]);
		
		lcm(minsup, new TIntHashSet(), dataset);
	}
	
	public static ArrayList<TIntSet> readFile(String path) {
		ArrayList<TIntSet> dataset = new ArrayList<TIntSet>();
		File file = new File(path);
		
		Scanner scanner;
		try {
			scanner = new Scanner(file);
			
			while (scanner.hasNextLine()) {
				StringTokenizer tokens = new StringTokenizer(scanner.nextLine());
				TIntSet transaction = new TIntHashSet();
				
				while (tokens.hasMoreTokens()) {
					transaction.add(Integer.parseInt(tokens.nextToken()));
				}
			}
			
			scanner.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		return dataset;
	}
	
	
	public static void lcm(int minsup, TIntSet pattern, ArrayList<TIntSet> dataset) {
		
		for (int i = 0; i < dataset.size(); i++) {
			System.out.println(dataset.get(i).toString());
		}
		
		System.out.println("READING OK");
		
	}
	
	public static TIntSet extractCandidates(ArrayList<TIntSet> dataset, int minsup, int max) {
		TIntSet candidates = new TIntHashSet();
		TIntLongHashMap supportPerItem = new TIntLongHashMap();
		
		/*TIntProcedure incrementItemSupport = new TIntProcedure() {
			@Override
			public boolean execute(int value) {
				
			}
		}
		
		for (TIntSet transaction : dataset) {
			
			transaction.forEach(incrementItemSupport);
			
			
			for (TIntSet tIntSet : transaction) {
				
			}
			
			if (supportPerItem.increment(key))
		}
		
		*/
		return candidates;
	}
}
