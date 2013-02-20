package fr.liglab.lcm.io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

import fr.liglab.lcm.internals.ItemsetsFactory;

/**
 * Reads transactions from an ASCII text file (\n-terminated)
 * Each line is a transaction, containing space-separated item IDs as integers
 * 
 * It directly implements the transactions iterator, but don't forget to call close() when finished !
 */
public class FileReader implements Iterator<int[]> {
	
	private BufferedReader inBuffer;
	private ItemsetsFactory builder = new ItemsetsFactory();
	private int[] nextArray;
	
	public FileReader(final String path) {
		try {
			inBuffer = new BufferedReader(new java.io.FileReader(path));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		fillNextArray();
	}
	
	public void close() {
		try {
			inBuffer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	private void fillNextArray() {
		nextArray = null;
		
		try {
			int nextInt = -1;
			int nextChar = inBuffer.read();
			
			while(true) {
				if (nextChar == -1) { // EOF
					if (!builder.isEmpty()) {
						nextArray = builder.get();
					}
					return;
					
				} else if (nextChar == '\n') { // EOL - skip possible empty lines
					
					if (!builder.isEmpty()) {
						nextArray = builder.get();
					}
					
					while (nextChar == '\n') {
						inBuffer.mark(2);
						nextChar = inBuffer.read();
					}
					
					inBuffer.reset();
					
					return;
					
				} else if (nextChar == ' ') {
					nextChar = inBuffer.read();
					
				} else {
					nextInt = -1;
					
					while('0' <= nextChar && nextChar <= '9') {
						if (nextInt < 0) {
							nextInt = nextChar - '0';
						} else {
							nextInt = (10*nextInt) + (nextChar - '0');
						}
						nextChar = inBuffer.read();
					}
					
					if (nextInt >= 0) {
						builder.add(nextInt);
					}
				}
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean hasNext() {
		return nextArray != null;
	}

	public int[] next() {
		int[] current = nextArray;
		fillNextArray();
		return current;
	}

	public void remove() {
		throw new NotImplementedException();
	}
}
