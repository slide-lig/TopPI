package fr.liglab.lcm.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import fr.liglab.lcm.internals.Itemset;
import fr.liglab.lcm.internals.Transactions;

public class FileReader {
	public static Transactions fromClassicAscii(String path) {
		Transactions dataset = new Transactions();
		File file = new File(path);
		
		try {
			Scanner scanner = new Scanner(file);
			
			while (scanner.hasNextLine()) {
				Itemset transaction = new Itemset();
				String[] tokens = scanner.nextLine().split(" ");
				
				for (String token : tokens) {
					transaction.add(Integer.parseInt(token));
				}
				
				dataset.add(transaction);
			}
			
			scanner.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		return dataset;
	}
}
