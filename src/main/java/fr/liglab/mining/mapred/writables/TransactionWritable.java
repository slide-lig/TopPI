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

import org.apache.hadoop.io.Writable;

public class TransactionWritable implements Writable {

	protected int[] transaction;

	public TransactionWritable() {
		super();
		transaction = new int[0];
	}

	public TransactionWritable(int[] t) {
		super();
		transaction = t;
	}

	public int[] get() {
		return transaction;
	}

	public int getLength() {
		return transaction.length;
	}

	public void set(int[] t) {
		transaction = t;
	}

	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		transaction = new int[length];

		for (int i = 0; i < transaction.length; i++) {
			transaction[i] = in.readInt();
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(transaction.length);
		for (int i = 0; i < transaction.length; i++) {
			out.writeInt(transaction[i]);
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		String space = "";

		for (int i = 0; i < transaction.length; i++) {
			builder.append(space);
			builder.append(transaction[i]);
			space = " ";
		}

		return builder.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TransactionWritable) {
			TransactionWritable other = (TransactionWritable) obj;
			if (other.transaction.length == transaction.length) {
				for (int i = 0; i < transaction.length; i++) {
					if (transaction[i] != other.transaction[i])
						return false;
				}
				return true;
			}
		}

		return false;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(this.transaction);
	}
}
