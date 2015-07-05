package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.util.*;

/**	Sorted array list.
 *
 *	<p>
 *	This class extends ArrayList to support sorted lists.
 *	</p>
 *
 *	<p>
 *	All elements of the list should implement the Comparable interface,
 *	or you may supply a custom Comparator instead.  If the objects do not
 *	support comparable and you do not supply a custom Comparator, the
 *	result of the toString() function for each object is used.
 *	</p>
 *
 *	<p>
 *	The ArrayList methods for adding or settings elements at specific indices
 *	are not supported raise an UnsupportedOperationException if used.
 *	</p>
 */

public class SortedArrayList extends ArrayList
{
	/** Optional Comparator used for comparing objects in the list.
	 */

	protected Comparator comparator	= null;

	/**	Create empty sorted array list.
	 */

	public SortedArrayList()
	{
		super();
	}

	/**	Create empty sorted array list with specified initial capacity.
	 */

	public SortedArrayList( int initialCapacity )
	{
		super( initialCapacity );
	}

	/**	Create sorted array list from a collection.
	 *
	 *	@param	collection	The source collection.
	 */

	public SortedArrayList( Collection collection )
	{
								//	Create empty list.
		super();

		addAll( collection );
	}

	/**	Create sorted array list from an array.
	 *
	 *	@param	array	The source array.
	 */

	public SortedArrayList( Object[] array )
	{
								//	Create empty list.
		super();

		addAll( Arrays.asList( array ) );
	}

	/**	Create empty sorted array list with specified Comparator.
	 */

	public SortedArrayList( Comparator comparator )
	{
		super();

		this.comparator	= comparator;
	}

	/**	Create sorted array list from a collection with specified comparator.
	 *
	 *	@param	collection	The source collection.
	 *	@param	comparator	The comparator.
	 */

	public SortedArrayList( Collection collection , Comparator comparator )
	{
								//	Create empty list.
		super();
								//	Set comparator.

		this.comparator	= comparator;

								//	Add all elements from collection.
		addAll( collection );
	}

	/**	Create sorted array list from an array with specified comparator.
	 *
	 *	@param	array		The source array.
	 *	@param	comparator	The comparator.
	 */

	public SortedArrayList( Object[] array , Comparator comparator )
	{
								//	Create empty list.
		super();
								//	Set comparator.

		this.comparator	= comparator;

								//	Add all elements from array.

		addAll( Arrays.asList( array ) );
	}

	/**	Create empty sorted array list with specified initial capacity
	 *	and comparator.
	 */

	public SortedArrayList( int initialCapacity , Comparator comparator )
	{
		super( initialCapacity );

		this.comparator	= comparator;
	}

	/**	Set comparator for list elements.
	 *
	 *	@param	comparator		The comparator.
	 */

	public void setComparator( Comparator comparator )
	{
		this.comparator	= comparator;
	}

	/**	Add an object to the list.
	 *
	 *	@param	object	Object to add.
	 *
	 *	@return			True if object added.
	 */

	public boolean add( Object object )
	{
								//	Don't allow adding null objects.

		if ( object == null ) return false;

								//	Binary search to find insertion point.
		int i	= 0;
		int j	= size() - 1;

		while ( i <= j )
		{
			int k	= ( i + j ) / 2;
			int c	= compare( object , get( k ) );

			if ( c < 0 )
			{
				j	= k - 1;
			}
			else if ( c > 0 )
			{
				i	= k + 1;
			}
			else
			{
				i	= k;
				break;
			}
		}
								//	Add object at correct position in list.

		super.add( i , object );

		return true;
	}

	/**	Add element at specified index.  Not allowed.
	 */

	public void add( int index , Object object )
	{
		throw new UnsupportedOperationException();
	}

	/**	Add all elements of a collection.
	 *
	 *	@param	collection	The source collection.
	 *
	 *	@return				True if all objects added, false otherwise.
	 */

	public boolean addAll( Collection collection )
	{
		boolean result	= true;

		if ( collection != null )
		{
			for ( Iterator i = collection.iterator() ; i.hasNext(); )
			{
				if ( !add( i.next() ) ) result	= false;
			}
		}

		return result;
	}

	/**	Add all elements of an array.
	 *
	 *	@param	array		The source array.
	 *
	 *	@return				True if all objects added, false otherwise.
	 */

	public boolean addAll( Object[] array )
	{
		boolean	result	= true;

		if ( array != null )
		{
			for ( int i = 0 ; i < array.length ; i++ )
			{
				if ( !add( array[ i ] ) ) result	= false;
			}
		}

		return result;
	}

	/**	Add all elements of a collection at a specified index.  Not allowed.
	 */

	public boolean addAll( int index , Collection collection )
	{
		throw new UnsupportedOperationException();
	}

	/** Determine if list contains a specified value.
	 *
	 *	@param	object	The object (value) to find.
	 *
	 *	@return			true if the list contains specified object,
	 *					false otherwise.
	 */

	public boolean contains( Object object )
	{
		return ( indexOf( object ) >= 0 );
	}

	/**	Return index of first matching list entry.
	 *
	 *	@param	object	The object to find.
	 *
	 *	@return			Index of the first matching object, if any.
	 *					Returns -1 if no matching object is found.
	 */

	public int indexOf( Object object )
	{
		int result	= -1;
								//	Null object returns -1.

		if ( object == null ) return result;

								//	Binary search to find matching object.
		int i	= 0;
		int j	= size() - 1;

		while ( i <= j )
		{
			int k	= ( i + j ) / 2;
			int c	= compare( object , get( k ) );

			if ( c < 0 )
			{
				j	= k - 1;
			}
			else if ( c > 0 )
			{
				i	= k + 1;
			}
			else
			{
								//	We found a matching object.
								//	Walk backwards through the array list
								//	to make sure it is the first one.
				result	= k;
				int l	= k - 1;

				while	(	( l >= 0 ) &&
							( compare( object , get( l ) ) == 0 ) ) l--;

				if ( ( l + 1 ) < result ) result	= l + 1;

				break;
			}
		}

		return result;
	}

	/**	Return index of last matching list entry.
	 *
	 *	@param	object	The object to find.
	 *
	 *	@return			Index of the last matching object, if any.
	 *					Returns -1 if no matching object is found.
	 */

	public int lastIndexOf( Object object )
	{
		int result	= -1;
								//	Null object returns -1.

		if ( object == null ) return result;

								//	Binary search to find matching object.
		int i	= 0;
		int j	= size() - 1;

		while ( i <= j )
		{
			int k	= ( i + j ) / 2;
			int c	= compare( object , get( k ) );

			if ( c < 0 )
			{
				j	= k - 1;
			}
			else if ( c > 0 )
			{
				i	= k + 1;
			}
			else
			{
								//	We found a matching object.
								//	Walk forwards through the array list
								//	to find the last matching object.
				result	= k;
				int l	= k + 1;

				while	(	( l < size() ) &&
							( compare( object , get( l ) ) == 0 ) ) l++;

				if ( ( l - 1 ) > result ) result	= l - 1;

				break;
			}
		}

		return result;
	}

	/**	Set specified element of list.  Not allowed.
	 */

	public Object set( int index , Object object )
	{
		throw new UnsupportedOperationException();
	}

	/** Compares two elements.
	 *
	 *	@param	object1		First object.
	 *	@param	object2		Second object.
	 *
	 *	@return				Result of comparison.
	 *
	 *	<p>
	 *	Assumes the two objects are not null, since null objects cannot
	 *	added to the list.  If no comparator is defined for this
	 *	SortedArrayList, and one or both of the objects are not comparable,
	 *	a case-ignoring comparison is performed on the "toString()" values
	 *	for each object.
	 *	</p>
	 */

	protected int compare( Object object1 , Object object2 )
	{
		if ( comparator != null )
		{
			return comparator.compare( object1 , object2 );
		}
		else if (	( object1 instanceof Comparable ) &&
					( object2 instanceof Comparable ) )
		{
			return ( (Comparable)object1 ).compareTo( (Comparable)object2 );
		}
		else
		{
			String s1	= null;
			String s2	= null;

			if ( object1 != null ) s1 = object1.toString();
			if ( object2 != null ) s2 = object2.toString();

//			return StringUtils.compare( s1 , s2 );
			return StringUtils.compareIgnoreCase( s1 , s2 );
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

