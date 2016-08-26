/*
	This file is part of TopPI - see https://github.com/slide-lig/TopPI/
	
	Copyright 2016 Martin Kirchgessner, Vincent Leroy, Alexandre Termier, Sihem Amer-Yahia, Marie-Christine Rousset, Université Grenoble Alpes, LIG, CNRS
	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	 http://www.apache.org/licenses/LICENSE-2.0
	 
	or see the LICENSE.txt file joined with this program.
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/
package fr.liglab.mining.mapred.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;


public final class ConcatenatedTransactionsWritable implements Writable {
	
	private int toBeWrittenConcatenatedLengths;
	private List<int[]> toBeWritten = null;
	private int[] fromDisk = null;
	
	public ConcatenatedTransactionsWritable() {}
	
	@Override
	public String toString() {
		if (this.fromDisk != null) {
			return Arrays.toString(this.fromDisk);
		} else {
			return "[not written yet]";
		}
	}
	
	/**
	 * @param buffer a list of transactions to be written to disk
	 * @param lengths cumulated length of the provided transactions
	 */
	public void set(List<int[]> buffer, int lengths) {
		this.toBeWritten = buffer;
		this.toBeWrittenConcatenatedLengths = lengths;
	}
	
	public int[] get() {
		return this.fromDisk;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		this.fromDisk = new int[length];
		
		for (int i = 0; i < length; i++) {
			this.fromDisk[i] = in.readInt();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (this.toBeWritten != null) {
			out.writeInt(this.toBeWrittenConcatenatedLengths + this.toBeWritten.size());
			
			Iterator<int[]> it = this.toBeWritten.iterator();
			while (it.hasNext()) {
				final int[] transaction = it.next();
				out.writeInt(transaction.length);
				
				for (int item : transaction) {
					out.writeInt(item);
				}
			}
		} else {
			out.writeInt(this.fromDisk.length);
			for (int i = 0; i < this.fromDisk.length; i++) {
				out.writeInt(this.fromDisk[i]);
			}
		}
	}

}
