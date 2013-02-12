package fr.liglab.lcm.io;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;



/**
 * a PatternsCollector that write to the path provided at instanciation
 * It will throw an exception if output file already exists
 */
public class FileCollector implements PatternsCollector {
	
	// this should be profiled and tuned !
	protected static int BUFFER_CAPACITY = 4096;
	// below this remaining capacity in buffer, it will be flushed to file
	protected static int BUFFER_THRESHOLD = 20; 
	
	protected FileOutputStream stream;
	protected FileChannel channel;
	protected ByteBuffer buffer;
	
	public FileCollector(String path) throws IOException {
		File file = new File(path);
		
		if (file.exists()) {
			throw new IOException(path + " already exists.");
		}
		
		stream = new FileOutputStream(file, false);
		channel = stream.getChannel();
		
		buffer = ByteBuffer.allocateDirect(BUFFER_CAPACITY);
		buffer.clear();
	}
	
	public void collect(int support, int[] pattern) {
		putInt(support);
		buffer.putChar('\t');
		
		if (buffer.remaining() < BUFFER_THRESHOLD) {
			flush();
		}
		
		boolean addSeparator = false;
		for (int item : pattern) {
			putInt(item);
			if (addSeparator) {
				buffer.putChar(' ');
			} else {
				addSeparator = true;
			}
			
			if (buffer.remaining() < BUFFER_THRESHOLD) {
				flush();
			}
		}
		
		buffer.putChar('\n');	
	}
	
	protected void putInt(int i) {
		for (char c : Integer.toString(i).toCharArray()) {
			buffer.putChar(c);
		}
	}
	
	protected void flush() {
		try {
			channel.write(buffer);
		} catch (IOException e) {
			e.printStackTrace(System.err);
		}
		
		buffer.clear();
	}

	public void close() {
		try {
			flush();
			channel.close();
			stream.close();
		} catch (IOException e) {
			e.printStackTrace(System.err);
		}
	}

}
