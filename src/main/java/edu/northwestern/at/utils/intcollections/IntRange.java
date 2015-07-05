package edu.northwestern.at.utils.intcollections;

/*	Please see the license information at the end of this file. */

import java.util.*;
import java.io.*;

/**	A range of integers.
 */

public class IntRange implements Serializable, Cloneable {

	/**	The first integer in the range. */
	
	public int first;
	
	/**	The last integer in the range + 1. */
	
	public int last;
	
	/**	Constructs a new integer range.
	 *
	 *	@param	first		The first integer in the range.
	 *
	 *	@param	last		The last integer in the range + 1.
	 *
	 *	@throws	IllegalArgumentException	If first >= last.
	 */
	 
	public IntRange (int first, int last) {
		if (first >= last) throw new IllegalArgumentException(
			"first >= last");
		this.first = first;
		this.last = last;
	}
	
	/**	Constructs a new integer range from a string.
	 *
	 *	@param	str		The string.
	 *
	 *	@throws	IllegalArgumentException	If syntax error in string.
	 */
	 
	public IntRange (String str) {
		if (str == null) throw new IllegalArgumentException(
			"argument is null");
		try {
			int i = str.indexOf('-');
			if (i < 0) {
				first = Integer.parseInt(str);
				last = first+1;
			} else {
				first = Integer.parseInt(str.substring(0,i));
				last = Integer.parseInt(str.substring(i+1)) + 1;
				if (first >= last) throw new IllegalArgumentException(
					"first >= last");
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("syntax error");
		}
	}
	
	/**	Returns a string representation of the range.
	 *
	 *	<p>If last = first+1: "first".
	 *
	 *	<p>If last > first+1: "first-xxx" where "xxx"is last-1.
	 *
	 *	@return		The integer range as a string.
	 */
	 
	public String toString () {
		if (last == first+1) {
			return Integer.toString(first);
		} else {
			return Integer.toString(first) + "-" +
				Integer.toString(last-1);
		}
	}
	
	/**	Returns a copy of the range.
	 *
	 *	@return		The copy.
	 */
	
	public Object clone () {
		return new IntRange(first, last);
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

