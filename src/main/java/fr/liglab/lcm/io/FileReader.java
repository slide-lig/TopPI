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
	private String[] nextTokens;
	
	public FileReader(String path) {
		File file = new File(path);
		
		try {
			scanner = new Scanner(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		fillNextTokens();
	}
	
	public void close() {
		scanner.close();
	}
	
	private void fillNextTokens() {
		nextTokens = null;
		
		while (nextTokens == null && scanner.hasNextLine()) {
			String[] tokens = scanner.nextLine().split(" ");
			
			if (tokens.length > 0 && !tokens[0].isEmpty()) {
				nextTokens = tokens;
			}
		}
	}

	public boolean hasNext() {
		return nextTokens != null;
	}

	public int[] next() {
		for (String token : nextTokens) {
			builder.add(Integer.parseInt(token));
		}
		
		fillNextTokens();
		
		return builder.get();
	}

	public void remove() {
		throw new NotImplementedException();
	}
}
