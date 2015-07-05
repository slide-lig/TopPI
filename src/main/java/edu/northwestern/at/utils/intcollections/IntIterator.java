package edu.northwestern.at.utils.intcollections;

/*	Please see the license information at the end of this file. */

/**	An iterator over integers. */

public interface IntIterator {

	/**	Returns true if the iterator has more elements. 
	 *
	 *	@return		True if iterator has more elements.
	 */

	public boolean hasNext ();
	
	/**	Returns the next element in the iterator.
	 *
	 *	@return		The next element.
	 *
	 *	@throws		NoSuchElementException	The iteration has no
	 *										more elements.
	 */
	
	public int next ();

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

