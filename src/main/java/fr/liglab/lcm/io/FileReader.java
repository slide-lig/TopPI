package fr.liglab.lcm.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

import fr.liglab.lcm.internals.ItemsetsFactory;

/**
 * Reads transactions from a classic text file
 * Each line is a transaction, containing space-separated item IDs as integers
 * 
 * It directly implements the transactions iterator, but don't forget to call close() when finished !
 */
public class FileReader implements Iterator<int[]> {
	
	private BufferedReader inBuffer;
	private ItemsetsFactory builder = new ItemsetsFactory();
	private String[] nextTokens;
	
	public FileReader(final String path) {
		try {
			inBuffer = new BufferedReader(new java.io.FileReader(path));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		fillNextTokens();
	}
	
	public void close() {
		try {
			inBuffer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void fillNextTokens() {
		try {
			nextTokens = null;
			String nextLine = inBuffer.readLine();
		
			while (nextTokens == null && nextLine != null) {
				String[] tokens = nextLine.split(" ");
				
				if (tokens.length > 0 && !tokens[0].isEmpty()) {
					nextTokens = tokens;
				} else {
					nextLine = inBuffer.readLine();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
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
