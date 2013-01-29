package fr.liglab.lcm;

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
