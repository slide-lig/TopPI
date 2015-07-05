package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.util.*;

/**	Extended array list.
 *
 *	<p>
 *	This class extends ArrayList to support array input better.
 *	</p>
 */

public class XArrayList extends ArrayList
{
	/**	Create empty array list.
	 */

	public XArrayList()
	{
		super();
	}

	/**	Create empty array list with specified initial capacity.
	 */

	public XArrayList( int initialCapacity )
	{
		super( initialCapacity );
	}

	/**	Create array list from a collection.
	 *
	 *	@param	collection	The source collection.
	 */

	public XArrayList( Collection collection )
	{
		super( collection );
	}

	/**	Create array list from an array.
	 *
	 *	@param	array	The source array.
	 */

	public XArrayList( Object[] array )
	{
								//	Create empty list.
		super();

		addAll( array );
	}

	/**	Add all elements of an array.
	 *
	 *	@param	array		The source array.
	 */

	public void addAll( Object[] array )
	{
		if ( array != null )
		{
			for ( int i = 0 ; i < array.length ; i++ )
			{
				add( array[ i ] );
			}
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

