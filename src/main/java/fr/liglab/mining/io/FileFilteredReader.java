package fr.liglab.mining.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataInputStream;

import fr.liglab.mining.internals.TransactionReader;
import fr.liglab.mining.mapred.Grouper.SingleGroup;
import gnu.trove.map.TIntIntMap;

/**
 * FileReader, copied, applying a renaming as soon as the first pass and filtering per group
 */
public final class FileFilteredReader implements Iterator<TransactionReader> {
	
	/**
	 * We avoid small allocations by using megabyte pages. Transactions are stored in pages 
	 * as in ConcatenatedTransactionsList, although lastest indexes may not be used.
	 */
	private static final int COPY_PAGES_SIZE = 1024*1024;
	
	private final ArrayList<int[]> pages = new ArrayList<int[]>();
	private Iterator<int[]> pagesIterator;
	private int[] currentPage;
	private int currentPageIndex;
	private int currentTransIdx;
	private int currentTransLen;
	private int[] renaming = null;
	private TIntIntMap initRenaming = null;
	private final CopyReader copyReader = new CopyReader();
	private CopyReader nextCopyReader = new CopyReader();
	private SingleGroup grouper;
	
	private MyReader inBuffer;
	private final LineReader lineReader = new LineReader();
	private int nextChar = 0;
	
	public FileFilteredReader(final String path, TIntIntMap globalRenaming, SingleGroup filter) {
		try {
			inBuffer = new BufferedReaderWrapper(new BufferedReader(new java.io.FileReader(path)));
			nextChar = inBuffer.read();
			initRenaming = globalRenaming;
			grouper = filter;
			
			newPage();
			preReadNext();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public FileFilteredReader(FSDataInputStream inputStream, TIntIntMap globalRenaming, SingleGroup filter) {
		try {
			inBuffer = new HDFSWrapper(inputStream);
			nextChar = inBuffer.read();
			initRenaming = globalRenaming;
			grouper = filter;
			
			newPage();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void newPage() {
		currentPage = new int[COPY_PAGES_SIZE];
		pages.add(currentPage);
		
		currentPageIndex = 1;
		currentTransIdx = 0;
		currentTransLen = 0;
	}
	
	private void writeNewTransactionToNextPage() {
		if (currentTransLen+1 >= COPY_PAGES_SIZE) {
			throw new RuntimeException("Inputted transactions are too long ! Try increasing " +
					"FileReader.COPY_PAGES_SIZE");
		}
		
		int[] previousPage = currentPage;
		
		currentPage = new int[COPY_PAGES_SIZE];
		pages.add(currentPage);
		
		previousPage[currentTransIdx] = -1;
		System.arraycopy(previousPage, currentTransIdx+1, currentPage, 1, currentTransLen);
		
		currentTransIdx = 0;
		currentPageIndex = currentTransLen+1;
	}

	public void close() {
		close(null);
	}
	
	public void close(int[] renamingMap) {
		try {
			inBuffer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		inBuffer = null;
		renaming = renamingMap;
		
		// last char should have been a '\n' so currentPageIndex was ready to write a new one
		currentPage[currentTransIdx] = -1;
		
		pagesIterator = pages.iterator();
		currentPage = null;
		
		nextCopyReader = new CopyReader();
		prepareNextCopyReader();
	}

	private void prepareNextCopyReader() {
		if (currentPage == null || currentTransIdx == COPY_PAGES_SIZE || 
				currentPage[currentTransIdx] == -1) {
			
			if (pagesIterator.hasNext()) {
				currentPage = pagesIterator.next();
				currentTransIdx = 0;
				
				if (currentPage[0] == -1) { // yes, it may happen !
					nextCopyReader = null;
					return;
				}
				
			} else {
				nextCopyReader = null;
				return;
			}
		}
		
		currentTransLen = currentPage[currentTransIdx];
		currentTransIdx++;
		
		if (renaming != null) {
			int filteredI = currentTransIdx;
			for (int i = currentTransIdx; i < currentTransIdx + currentTransLen; i++) {
				final int renamed = renaming[currentPage[i]];
				if (renamed >= 0) {
					currentPage[filteredI++] = renamed;
				}
			}
			
			if (filteredI == currentTransIdx) { // completely filtered transaction ! try again
				currentTransIdx += currentTransLen;
				prepareNextCopyReader();
			}
			
			//  Arrays.sort(currentPage, currentTransIdx, filteredI);
			this.nextCopyReader.setup(currentPage, currentTransIdx, filteredI);
		} else {
			this.nextCopyReader.setup(currentPage, currentTransIdx, currentTransIdx + currentTransLen);
		}
		
		currentTransIdx += currentTransLen;
	}

	public boolean hasNext() {
		return nextCopyReader != null;
	}
	
	public TransactionReader next() {
		if (nextCopyReader != null) {
			copyReader.setup(nextCopyReader.source, nextCopyReader.i, nextCopyReader.end);
			if (inBuffer == null) {
				prepareNextCopyReader();
			} else {
				preReadNext();
			}
		}
		return copyReader;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	private void preReadNext() {
		boolean tryAgain = true;
		
		if (nextChar == -1) {
			nextCopyReader = null;
			return;
		}
		
		while (tryAgain && nextChar != -1) {
			skipNewLines();
			
			int[] page = currentPage;
			int i = currentTransIdx;
			
			while (lineReader.hasNext()) {
				int next = lineReader.next();
				if (tryAgain && next >= 0 && grouper.getGroupId(next) >= 0) {
					tryAgain = false;
				}
			}
			
			if (currentPage != page) { // writeNewTransactionToNextPage occurred
				i = 0;
			}
			
			if (tryAgain) { // REWIND
				if (currentTransIdx < COPY_PAGES_SIZE) {
					currentPage[currentTransIdx] = -1;
				}
				currentPageIndex = i+1;
				currentTransIdx = i;
			} else {
				nextCopyReader.setup(currentPage, i+1, currentPage[i]+i+1);
				
				if (currentTransIdx >= COPY_PAGES_SIZE) {
					newPage();
				}
			}
			skipNewLines();
		}
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
	
	
	
	private final class LineReader implements TransactionReader {

		@Override
		public int getTransactionSupport() {
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
			
			if (currentPageIndex == COPY_PAGES_SIZE) {
				writeNewTransactionToNextPage();
			}
			
			nextInt = initRenaming.get(nextInt);
			if (nextInt >= 0) {
				currentPage[currentPageIndex++] = nextInt;
				currentTransLen++;
			}
			
			if (nextChar == '\n') {
				currentPage[currentTransIdx] = currentTransLen;
				currentTransIdx = currentPageIndex++;
				currentTransLen = 0;
			}
			
			return nextInt;
		}

		@Override
		public boolean hasNext() {
			return (nextChar != '\n');
		}
	}
	
	private final class CopyReader implements TransactionReader {
		
		private int[] source;
		private int i;
		private int end;
		
		/**
		 * read currentPage[currentPageIndex, to[
		 */
		private void setup(int[] array, int from, int to){
			source = array;
			i = from;
			end = to;
		}
		
		@Override
		public int getTransactionSupport() {
			return 1;
		}

		@Override
		public int next() {
			return source[i++];
		}

		@Override
		public boolean hasNext() {
			return i < end;
		}
		
	}
	
	private interface MyReader {
		void close() throws IOException;
		int read() throws IOException;
	}
	
	private final class BufferedReaderWrapper implements MyReader{
		private final BufferedReader wrapped;
		
		BufferedReaderWrapper(BufferedReader source) {
			this.wrapped = source;
		}
		
		@Override public void close() throws IOException { this.wrapped.close(); }
		@Override public int read() throws IOException { return this.wrapped.read(); }
	}

	private final class HDFSWrapper implements MyReader{
		private final FSDataInputStream wrapped;
		
		HDFSWrapper(FSDataInputStream source) {
			this.wrapped = source;
		}
		
		@Override public void close() throws IOException { this.wrapped.close(); }
		@Override public int read() throws IOException { return this.wrapped.read(); }
	}
}
