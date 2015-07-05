package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.util.*;

/**	ArgumentList.
 *
 *	<p>ArgumentList provides methods for building an argument list for use
 *	in a {@link DynamicCall}.</p>
 */

public class ArgumentList
{
    /**	Class for each argument.
     */

	protected Class[] classes;

	/** Argument values.  Each argument must be an object or
	 *	wrapped in an object.
	 */

	protected Object[] arguments;

	/** Count of arguments.
	 */

	protected int argumentCount;

	/** Construct empty argument for method taking no parameters.
	 */

	public ArgumentList()
	{
		argumentCount	= 0;
		classes			= new Class[ 0 ];
		arguments		= new Object[ 0 ];
	}

	/** Construct argument list which can hold specified number of parameters.
	 *
	 *	@param		argumentCount	The number of parameters.
	 */

	public ArgumentList( int argumentCount )
	{
		this.argumentCount	= argumentCount;

		classes				= new Class[ argumentCount ];
		arguments 			= new Object[ argumentCount ];
	}

	/** Get classes corresponding to each argument.
	 *
	 *	@return		The list of argument classes.
	 */

	public Class[] getArgumentClasses()
	{
		return classes;
	}

	/** Get argument values as list of objects.
	 *
	 *	@return		The argument values.
	 */

	public Object[] getArguments()
	{
		return arguments;
	}

	/** Set value of specific argument.
	 *
	 *	@param	argumentNumber	Index of argument to set.
	 *	@param	object			The value of the argument as an object.
	 *	@param	theClass		The type of the argument as a class.
	 *
	 *	@return					The next available argument index.
	 */

	public int setArgument( int argumentNumber , Object object , Class theClass )
	{
									// If the index of the argument to be added
									// is past the length of the currently allocated
									// argument/class lists, expand the size by one.

		if ( argumentNumber >= arguments.length )
		{
			argumentCount++;

			Class[] classesExpanded		= new Class[ argumentCount ];
			Object[] argumentsExpanded	= new Object[ argumentCount ];

			System.arraycopy( classes	, 0 , classesExpanded	, 0 , classes.length );
			System.arraycopy( arguments	, 0 , argumentsExpanded	, 0 , arguments.length );

			classes		= classesExpanded;
			arguments	= argumentsExpanded;
		}
									// Add the new argument to the end of the list.

		arguments[ argumentNumber ]	= object;
		classes[ argumentNumber ]	= theClass;

									// Return the updated argument list size.
		return argumentNumber;
	}

	/** The following methods offer convenience ways for add arguments of primitive
	 *	types.
	 */

	/**	Set the next argument to a boolean value.
	 *
	 *	@param	b	The boolean value.
	 *
	 *	@return		The next available argument entry index.
	 *
	 */

	public int setArgument( boolean b )
	{
		return this.setArgument( argumentCount , new Boolean( b ) , Boolean.TYPE );
	}

	/**	Set specified argument to a boolean value.
	 *
	 *	@param	argumentNumber		The index of the argument to set.
	 *
	 *	@param	b					The boolean value.
	 *
	 *	@return						The next available argument entry index.
	 *
	 */

	public int setArgument( int argumentNumber , boolean b )
	{
		return this.setArgument( argumentNumber, new Boolean( b ), Boolean.TYPE );
	}

	/**	Set the next argument to a byte value.
	 *
	 *	@param	b	The byte value.
	 *
	 *	@return		The next available argument entry index.
	 *
	 */

	public int setArgument( byte b )
	{
		return this.setArgument( argumentCount , new Byte( b ) , Byte.TYPE );
	}

	/**	Set specified argument to a byte value.
	 *
	 *	@param	argumentNumber		The index of the argument to set.
	 *
	 *	@param	b					The byte value.
	 *
	 *	@return						The next available argument entry index.
	 *
	 */

	public int setArgument( int argumentNumber , byte b )
	{
		return this.setArgument( argumentNumber, new Byte( b ), Byte.TYPE );
	}

	/**	Set the next argument to a character value.
	 *
	 *	@param	c	The character value.
	 *
	 *	@return		The next available argument entry index.
	 *
	 */

	public int setArgument( char c )
	{
		return this.setArgument( argumentCount, new Character( c ), Character.TYPE );
	}

	/**	Set specified argument to a char value.
	 *
	 *	@param	argumentNumber		The index of the argument to set.
	 *
	 *	@param	c					The char value.
	 *
	 *	@return						The next available argument entry index.
	 *
	 */

	public int setArgument( int argumentNumber , char c )
	{
		return this.setArgument( argumentNumber , new Character(c),Character.TYPE );
	}

	/**	Set the next argument to an integer value.
	 *
	 *	@param	i	The integer value.
	 *
	 *	@return		The next available argument entry index.
	 *
	 */

	public int setArgument( int i )
	{
		return this.setArgument( argumentCount , new Integer( i ) , Integer.TYPE );
	}

	/**	Set specified argument to an integer value.
	 *
	 *	@param	argumentNumber		The index of the argument to set.
	 *
	 *	@param	i					The integer value.
	 *
	 *	@return						The next available argument entry index.
	 *
	 */

	public int setArgument( int argumentNumber , int i )
	{
		return this.setArgument( argumentNumber , new Integer( i ) , Integer.TYPE );
	}

	/**	Set the next argument to a short value.
	 *
	 *	@param	s	The short value.
	 *
	 *	@return		The next available argument entry index.
	 *
	 */

	public int setArgument( short s )
	{
		return this.setArgument( argumentCount , new Short( s ) , Short.TYPE );
	}

	/**	Set specified argument to a short value.
	 *
	 *	@param	argumentNumber		The index of the argument to set.
	 *
	 *	@param	s					The short value.
	 *
	 *	@return						The next available argument entry index.
	 *
	 */

	public int setArgument( int argumentNumber , short s )
	{
		return this.setArgument( argumentNumber , new Short( s ) , Short.TYPE );
	}

	/**	Set the next argument to a long value.
	 *
	 *	@param	l	The long value.
	 *
	 *	@return		The next available argument entry index.
	 *
	 */

	public int setArgument( long l )
	{
		return this.setArgument( argumentCount , new Long( l ) , Long.TYPE );
	}

	/**	Set specified argument to a long value.
	 *
	 *	@param	argumentNumber		The index of the argument to set.
	 *
	 *	@param	l					The long value.
	 *
	 *	@return						The next available argument entry index.
	 *
	 */

	public int setArgument( int argumentNumber , long l )
	{
		return this.setArgument( argumentNumber , new Long( l ) , Long.TYPE );
	}

	/**	Set the next argument to a float value.
	 *
	 *	@param	f	The float value.
	 *
	 *	@return		The next available argument entry index.
	 *
	 */

	public int setArgument( float f )
	{
		return this.setArgument( argumentCount , new Float( f ) , Float.TYPE );
	}

	/**	Set specified argument to a float value.
	 *
	 *	@param	argumentNumber		The index of the argument to set.
	 *
	 *	@param	f					The float value.
	 *
	 *	@return						The next available argument entry index.
	 *
	 */

	public int setArgument( int argumentNumber , float f )
	{
		return this.setArgument( argumentNumber , new Float( f ) , Float.TYPE );
	}

	/**	Set the next argument to a double value.
	 *
	 *	@param	d	The double value.
	 *
	 *	@return		The next available argument entry index.
	 *
	 */

	public int setArgument( double d )
	{
		return this.setArgument( argumentCount , new Double( d ) , Double.TYPE );
	}

	/**	Set specified argument to a double value.
	 *
	 *	@param	argumentNumber		The index of the argument to set.
	 *
	 *	@param	d					The double value.
	 *
	 *	@return						The next available argument entry index.
	 *
	 */

	public int setArgument( int argumentNumber , double d )
	{
		return this.setArgument( argumentNumber , new Double( d ) , Double.TYPE );
	}

	/**	Set the next argument to an object.
	 *
	 *	@param	object	The object.
	 *
	 *	@return			The next available argument entry index.
	 *
	 */

	public int setArgument( Object object )
	{
		if ( object == null )
		{
			return this.setArgument(
				argumentCount , object , Object.class );
		}
		else
		{
			return this.setArgument(
				argumentCount , object , object.getClass() );
		}
	}

	/**	Set specified argument to an object.
	 *
	 *	@param	argumentNumber		The index of the argument to set.
	 *
	 *	@param	object				The object.
	 *
	 *	@return						The next available argument entry index.
	 *
	 */

	public int setArgument( int argumentNumber , Object object )
	{
		if ( object == null )
		{
			return this.setArgument(
				argumentNumber , object , Object.class );
		}
		else
		{
			return this.setArgument(
				argumentNumber , object , object.getClass() );
		}
	}

	/**	Sets the next argument to an object.
	 *
	 *	@param	object		The object.
	 *
	 *	@param	theClass	The class of the object.
	 *
	 *	@return				The next available argument entry index.
	 *
	 */

	public int setArgument( Object object, Class theClass )
	{
		return this.setArgument( argumentCount , object , theClass );
	}

	public String toString () {
		StringBuffer buf = new StringBuffer();
		buf.append("ArgumentList:\n");
		for (int i = 0; i < argumentCount; i++) {
			buf.append("   " + classes[i].getName() + " " + arguments[i] + "\n");
		}
		return buf.toString();
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

