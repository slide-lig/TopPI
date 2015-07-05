package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.util.*;
import edu.northwestern.at.utils.intcollections.*;

/**	An enhanced bitset. */

public class XBitSet extends BitSet {

	/**	The empty set. */

	public static final XBitSet EMPTY_SET = new XBitSet(1);

	/**	Constructs a new empty bitset. */

	public XBitSet () {
	}

	/**	Constructs a new empty bitset with enough room for nbits bits.
	 *
	 *	@param	nbits		The initial size of the bitset.
	 */

	public XBitSet (int nbits) {
		super(nbits);
	}

	/**	Constructs a new bitset from an array.
	 *
	 *	@param	bits	An array of the initial bits to set.
	 */

	public XBitSet (int[] bits) {
		for (int i = 0; i < bits.length; i++) set(bits[i]);
	}

	/**	Constructs a new bitset with enough room for nbits bits
	 *	from an array.
	 *
	 *	@param	nbits		The initial size of the bitset.
	 *
	 *	@param	bits		An array of the initial bits to set.
	 */

	public XBitSet (int nbits, int[] bits) {
		super(nbits);
		for (int i = 0; i < bits.length; i++) set(bits[i]);
	}

	/**	Gets the number of elements in the bitset.
	 *
	 *	@return		The number of elements.
	 */

	public int count () {
		int result = 0;
		for (int i = 0; i < size(); i++)
			if (get(i)) result++;
		return result;
	}

	/**	Gets an iterator over the bitset.
	 *
	 *	<p>The behavior of the iterator is unspecified if the underlying
	 *	bitset is modified while the iteration is in progress.
	 *
	 *	@return		The iterator.
	 */

	public IntIterator iterator () {
		return new IntIterator() {
			int pos = 0;
			int size = size();
			public boolean hasNext () {
				while (pos < size && !get(pos)) pos++;
				return pos < size;
			}
			public int next () {
				if (!hasNext()) throw new NoSuchElementException();
				int result = pos;
				pos++;
				return result;
			}
		};
	}

	/**	Or another XBitSet with this one.
	 *
	 *	@return		This bitset ORed with the specified bitset.
	 */

	public XBitSet or( XBitSet bitSet )
	{
		int orSize = size();

		if ( bitSet.size() > orSize )
			orSize = bitSet.size();

		XBitSet result = new XBitSet( orSize );

		for ( int i = 0; i < size(); i++ )
			result.set( i , get( i ) );

		for ( int i = 0; i < bitSet.size(); i++ )
			result.set( i , bitSet.get( i ) || get( i ) );

		return result;
	}

	/**	Sets the bit at a specificed index to a specified value.
	 *
	 *	@param	bitIndex		Bit index.
	 *
	 *	@param	value			The boolean value to set.
	 */

	public void set (int bitIndex, boolean value) {
		if (value) {
			set(bitIndex);
		} else {
			clear(bitIndex);
		}
	}
}

/*
 * <p>
 * Copyright &copy; 2004-2011 Northwestern University.
 * </p>
 * <p>
 * This program is free software; you can redistribute it
 * and/or modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * </p>
 * <p>
 * This program is distributed in the hope that it will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the GNU General Public License for more
 * details.
 * </p>
 * <p>
 * You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free
 * Software Foundation, Inc., 59 Temple Place, Suite 330,
 * Boston, MA 02111-1307 USA.
 * </p>
 */

