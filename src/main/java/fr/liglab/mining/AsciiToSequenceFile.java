package fr.liglab.mining;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import fr.liglab.mining.internals.TransactionReader;
import fr.liglab.mining.io.FileReader;
import fr.liglab.mining.mapred.writables.TransactionWritable;
import fr.liglab.mining.util.ItemsetsFactory;

/**
 * command-line tool that converts a traditional .dat file into a SequenceFile
 * 
 * USAGE: hadoop fr.liglab.mining.AsciiToSequenceFile INPUT OUTPUT
 */
public class AsciiToSequenceFile extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new AsciiToSequenceFile(), args);
	}
	
	public int run(String[] args) throws IOException {
		if (args.length != 2) {
			System.err.println("USAGE: hadoop fr.liglab.mining.AsciiToSequenceFile INPUT OUTPUT");
		}
		
		FileSystem fs = FileSystem.get(getConf());
		Writer writer = new Writer(fs, getConf(), new Path(args[1]), NullWritable.class, TransactionWritable.class);
		
		NullWritable keyW = NullWritable.get();
		TransactionWritable valueW = new TransactionWritable();
		
		FileReader reader = new FileReader(args[0]);
		ItemsetsFactory factory = new ItemsetsFactory();
		
		while(reader.hasNext()) {
			TransactionReader source = reader.next();
			
			while(source.hasNext()) {
				factory.add(source.next());
			}
			
			valueW.set(factory.get());
			writer.append(keyW, valueW);
		}
		
		writer.close();
		reader.close();
		
		return 0;
	}

}
