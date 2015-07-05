package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.util.*;

/**	A list of Longs, typically used as database IDs.
 */

public class IDList
{
	/**	The IDs in the list, in order. */

	protected ArrayList ids	= new ArrayList();

	/**	Create an empty ID list.
	 */

	public IDList()
	{
	}

	/**	Create a list with specified IDs.
	 *
	 *	@param	ids		The IDs.
	 */

	public IDList( Long[] ids )
	{
		if ( ( ids == null ) || ( ids.length == 0 ) ) return;

		for ( int i = 0 ; i < ids.length ; i++ )
		{
			this.ids.add( ids[ i ] );
		}
	}

	/**	Create a list with specified IDs.
	 *
	 *	@param	ids		Collection of IDs.
	 */

	public IDList( Collection ids )
	{
		if ( ( ids == null ) || ( ids.size() == 0 ) ) return;

		for	(	Iterator iterator = ids.iterator();
		        iterator.hasNext() ;
			)
		{
			this.ids.add( (Long)iterator.next() );
		}
	}

	/**	Add an ID to the end of the list.
	 *
	 *	@param	id	The ID to add to the end of the list.
	 */

	public void add( Long id )
	{
		ids.add( id );
	}

	/**	Gets IDs.
	 *
	 *	@return		The IDs.
	 */

	public ArrayList getIDs()
	{
		return ids;
	}

	/**	Get the number of IDs.
	 *
	 *	@return		Number of IDs.
	 */

	public int getNumIDs()
	{
		return ids.size();
	}

	/**	Clears the ids.
	 */

	public void clear()
	{
		ids.clear();
	}

	/**	Gets a string representation of the phrase.
	 *
	 *	@return		The word tags in the phrase set.
	 */

	public String toString()
	{
		return ids.toString();
	}

	/**	Returns true if some other id list is equal to this one.
	 *
	 *	<p>The two lists are equal if their id entries are equal.</p>
	 *
	 *	@param	obj		The other object.
	 *
	 *	@return			True if this object equals the other object.
	 */

	public boolean equals( Object obj )
	{
		if ( ( obj == null ) || !( obj instanceof IDList ) ) return false;

		IDList other	= (IDList)obj;

		if ( other.getNumIDs() != getNumIDs() ) return false;

		ArrayList otherIDs	= other.getIDs();

		for ( int i = 0 ; i < ids.size() ; i++ )
		{
			if ( !ids.get( i ).equals( otherIDs.get( i ) ) )
			{
				return false;
			}
		}

		return true;
	}

	/**	Returns a hash code for the object.
	 *
	 *	@return		The hash code.
	 */

	public int hashCode()
	{
		int result	= 0;

		for ( int i = 0 ; i < ids.size() ; i++ )
		{
			result	= result * 37 + ((Long)ids.get( i )).hashCode();
		}

		return result;
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

