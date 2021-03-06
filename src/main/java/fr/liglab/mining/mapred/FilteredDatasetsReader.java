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
package fr.liglab.mining.mapred;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;

import fr.liglab.mining.internals.TransactionReader;
import fr.liglab.mining.mapred.Grouper.SingleGroup;
import fr.liglab.mining.mapred.writables.ConcatenatedTransactionsWritable;

public class FilteredDatasetsReader implements Iterable<TransactionReader> {

	private List<int[]> cached = new ArrayList<int[]>();
	private SingleGroup groupFilter;

	private List<URI> sourceFiles = null;
	private Configuration conf = null;

	public FilteredDatasetsReader(List<URI> files, Configuration conf, SingleGroup filter) {
		this.groupFilter = filter;
		this.sourceFiles = files;
		this.conf = conf;
	}

	@Override
	public Iterator<TransactionReader> iterator() {
		if (this.sourceFiles != null) {
			return new ArrayFilteredConcatenator(new SequenceFilesDecorator(this.sourceFiles.iterator()));
		} else {
			return new ArrayFilteredConcatenator(this.cached.iterator());
		}
	}

	private final class SequenceFilesDecorator implements Iterator<int[]> {

		private Iterator<URI> sources = null;
		private boolean hasNext;
		private Reader reader;
		private final NullWritable key = NullWritable.get();
		private final ConcatenatedTransactionsWritable value = new ConcatenatedTransactionsWritable();

		public SequenceFilesDecorator(Iterator<URI> sourceFiles) {
			this.sources = sourceFiles;
			this.nextFile();
		}

		@Override
		public boolean hasNext() {
			return this.hasNext;
		}

		private void nextLine() {
			try {
				this.hasNext = this.reader.next(this.key, this.value);

				if (!this.hasNext) {
					reader.close();
					this.nextFile();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void nextFile() {
			if (this.sources.hasNext()) {
				try {
					this.reader = new SequenceFile.Reader(conf, Reader.file(new Path(sources.next())));
				} catch (IOException e) {
					e.printStackTrace();
				}
				this.nextLine();
			} else {
				this.hasNext = false;
			}
		}

		@Override
		public int[] next() {
			int[] array = this.value.get();
			cached.add(array);
			this.nextLine();
			return array;
		}

		@Override
		public void remove() {
			throw new NotImplementedException();
		}
	}

	private final class ArrayFilteredConcatenator implements Iterator<TransactionReader> {

		private final Iterator<int[]> source;
		private ArrayReader reader = new ArrayReader();
		private ArrayReader currentReader = new ArrayReader();
		private int[] concatenated = null;
		private int next = 0;

		public ArrayFilteredConcatenator(Iterator<int[]> arrays) {
			this.source = arrays;
			this.forward();
		}

		@Override
		public boolean hasNext() {
			return this.reader != null;
		}

		private void forward() {
			while (this.source.hasNext() || this.next < this.concatenated.length) {
				if (this.concatenated == null || this.next == this.concatenated.length) {
					if (!this.source.hasNext()) {
						this.reader = null;
						return;
					}
					this.concatenated = this.source.next();
					this.next = 0;
				}

				final int start = this.next + 1;
				this.next += this.concatenated[this.next];

				if (this.reader.recycle(this.concatenated, start, this.next++)) {
					return;
				}
			}

			this.reader = null;
			return;
		}

		@Override
		public TransactionReader next() {
			this.currentReader.copy(this.reader);
			this.forward();
			return this.currentReader;
		}

		@Override
		public void remove() {
			throw new NotImplementedException();
		}

	}

	private final class ArrayReader implements TransactionReader {

		private int[] transaction = null;
		private int i = 0;
		private int last = -1;

		/**
		 * @param source
		 * @param from
		 *            first valid index in source
		 * @param to
		 *            last valid index in source
		 * @return false if no item belongs to the FilteredDatasetReader's group
		 */
		public boolean recycle(int[] source, int from, int to) {
			this.transaction = source;
			this.i = from;
			this.last = to;

			for (int i = from; i <= to; i++) {
				if (groupFilter.getGroupId(source[i]) >= 0) {
					return true;
				}
			}

			return false;
		}

		public void copy(ArrayReader other) {
			this.transaction = other.transaction;
			this.i = other.i;
			this.last = other.last;
		}

		@Override
		public int next() {
			return this.transaction[this.i++];
		}

		@Override
		public boolean hasNext() {
			return this.i <= this.last;
		}

		@Override
		public int getTransactionSupport() {
			return 1;
		}
	}
}
