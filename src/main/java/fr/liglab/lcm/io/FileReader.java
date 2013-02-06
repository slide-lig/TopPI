package fr.liglab.lcm.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.commons.lang.NotImplementedException;

import fr.liglab.lcm.internals.ItemsetsFactory;

/**
 * Reads transactions from a classic text file
 * Each line is a transaction, containing space-separated item IDs as integers
 * 
 * It directly implements the transactions iterator, but don't forget to call close() when finished !
 */
public class FileReader implements Iterator<int[]> {
	
	private Scanner scanner;
	private ItemsetsFactory builder = new ItemsetsFactory();
	
	public FileReader(String path) {
		File file = new File(path);
		
		try {
			scanner = new Scanner(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
	}
	
	public void close() {
		scanner.close();
	}

	public boolean hasNext() {
		return scanner.hasNextLine();
	}

	public int[] next() {
		String[] tokens = scanner.nextLine().split(" ");
		
		for (String token : tokens) {
			builder.add(Integer.parseInt(token));
		}
		
		return builder.get();
	}

	public void remove() {
		throw new NotImplementedException();
	}
}
