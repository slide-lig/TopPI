package fr.liglab.lcm.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

import fr.liglab.lcm.internals.TransactionReader;
import fr.liglab.lcm.util.ItemsetsFactory;

/**
 * Reads transactions from an ASCII text file (\n-terminated)
 * Each line is a transaction, containing space-separated item IDs as integers
 * (it does not read custom transaction IDs or weights) 
 * 
 * It directly implements the transactions iterator, but don't forget to call close() when finished.
 * 
 * It also keeps a copy of all transactions so you can re-iterate for dataset instanciation : take 
 * care to use it an leave it quickly afterwards to garbage collection.
 */
public class FileReader implements Iterator<TransactionReader>, Iterable<int[]> {
	
	private BufferedReader inBuffer;
	private final LineReader lineReader = new LineReader();
	private int nextChar = 0;
	private final ItemsetsFactory builder = new ItemsetsFactory();
	private final ArrayList<int[]> copy = new ArrayList<int[]>();
	
	public FileReader(final String path) {
		try {
			this.inBuffer = new BufferedReader(new java.io.FileReader(path));
			this.nextChar = this.inBuffer.read();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void close() {
		if (!this.builder.isEmpty()) {
			this.copy.add(this.builder.get());
		}
		try {
			this.inBuffer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public boolean hasNext() {
		skipNewLines();
		return this.nextChar != -1;
	}
	
	public TransactionReader next() {
		if (!this.builder.isEmpty()) {
			this.copy.add(this.builder.get());
		}
		skipNewLines();
		return this.lineReader;
	}

	public void remove() {
		throw new NotImplementedException();
	}
	
	private void skipNewLines() {
		try {
			while (this.nextChar == '\n') {
				this.nextChar = this.inBuffer.read();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Iterator<int[]> iterator() {
		return this.copy.iterator();
	}
	
	
	
	private class LineReader implements TransactionReader {

		@Override
		public final int getTransactionSupport() {
			return 1;
		}

		@Override
		public int next() {
			int nextInt = -1;
			try {
				while (nextChar == ' ')
					nextChar = inBuffer.read();
				
				while('0' <= nextChar && nextChar <= '9') {
					if (nextInt < 0) {
						nextInt = nextChar - '0';
					} else {
						nextInt = (10*nextInt) + (nextChar - '0');
					}
					nextChar = inBuffer.read();
				}
				
				while (nextChar == ' ')
					nextChar = inBuffer.read();
				
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			builder.add(nextInt);
			
			return nextInt;
		}

		@Override
		public boolean hasNext() {
			return (nextChar != '\n');
		}
	}
}
