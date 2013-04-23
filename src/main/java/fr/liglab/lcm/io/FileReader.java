package fr.liglab.lcm.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.NotImplementedException;

import fr.liglab.lcm.internals.TransactionReader;

/**
 * Reads transactions from an ASCII text file (\n-terminated)
 * Each line is a transaction, containing space-separated item IDs as integers
 * (it does not read custom transaction IDs or weights) 
 * 
 * It directly implements the transactions iterator, but don't forget to call close() when finished !
 */
public class FileReader implements Iterator<TransactionReader> {
	
	private BufferedReader inBuffer;
	private final LineReader lineReader = new LineReader();
	private int nextChar = 0;
	
	public FileReader(final String path) {
		try {
			inBuffer = new BufferedReader(new java.io.FileReader(path));
			nextChar = inBuffer.read();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void close() {
		try {
			inBuffer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public boolean hasNext() {
		skipNewLines();
		return nextChar != -1;
	}
	
	public TransactionReader next() {
		skipNewLines();
		return this.lineReader;
	}

	public void remove() {
		throw new NotImplementedException();
	}
	
	private void skipNewLines() {
		try {
			while (nextChar == '\n') {
				nextChar = inBuffer.read();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
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
			
			return nextInt;
		}

		@Override
		public boolean hasNext() {
			return (nextChar != '\n');
		}
	}
}
