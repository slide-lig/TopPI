package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.io.*;
import java.util.*;
import java.lang.reflect.*;

/**	Java class helpers.
 *
 *	<p>Provides methods for unmangling class names and retrieving
 *	information about classes (subclasses, interfaces implemented, etc.).</p>
 *
 *	<p>See also {@link DynamicCall} which allows dynamic loading of a
 *	class method with parameters from a named class.</p>
 */

public class ClassHelpers
{
	/**
	* Convert a mangled java name to the normal Java syntax equivalent.
	*
	* 	@param		name		The mangled name.
	*	@return					The unmangled name.
	*
	* <p>Arrays are indicated by one or more '[' characters (the count
	* indicates the number of "[]" pairs), followed by a single upper-case
	* letter denoting the array type.</p>
	*/

	public static String unmangleJavaName( String name )
	{
		int arrayCount;
									// Look for '[' characters at start of 'name'.

		for ( arrayCount = 0; arrayCount < name.length(); arrayCount++ )
		{
			if ( name.charAt( arrayCount ) != '[' )
			{
				break;
			}
		}
									// No '[' -- name doesn't need unmangling.
		if ( arrayCount == 0 )
		{
			return name;
		}
									// Find out what kind of array we have.
									// Append the type to the output.

		StringBuffer buf = new StringBuffer();

		switch ( name.charAt( arrayCount ) )
		{
			case 'B':
				buf.append( "byte" );
				break;

			case 'C':
				buf.append( "char" );
				break;

			case 'D':
				buf.append( "double" );
				break;

			case 'F':
				buf.append("float");
				break;

			case 'I':
				buf.append("int");
				break;

			case 'J':
				buf.append("long");
				break;

			case 'L':
                                // Find the ';' that terminates the type declaration.
                                // Place the proper number of "[]" symbols there,
                                // followed by a space and the remainder of the name.

				int semicolon = name.indexOf( ';' );

				buf.append( name.substring( arrayCount + 1 , semicolon ) );

				for ( int i = 0; i < arrayCount; i++ )
				{
					buf.append(" [ ]");
				}

				buf.append( ' ' );

				if ( ( semicolon + 1 ) <= ( name.length() - 1 ) )
				{
					buf.append( name.substring( semicolon + 1 , name.length() - 1 ) );
				}

				return buf.toString();

			case 'S':
				buf.append( "short" );
				break;

			case 'V':
				buf.append( "void" );
				break;

			case 'Z':
				buf.append( "boolean" );
				break;

			default:
				break;
		}
									// Append appropriate number of "[]" .

		for ( int i = 0; i < arrayCount; i++ )
		{
			buf.append( " [ ]" );
		}
									// Return unmangled name.
		return buf.toString();
	}

	/**
	 *	Removes class modifiers, leaving only the class name.
	 *
	 *	@param	name	The class name plus modifiers, if any.
	 *
	 *	@return			The class name stripped of modifiers.
	 *
	 */

	public static String trimClassName( String name )
	{
		String result;
									// Find last period in class name.

		int lastPeriod = name.lastIndexOf( '.' );

                                    // If found, strip everything
                                    // before it.  The result is the
                                    // unmodified class name.
                                    // e.g.,
                                    // a.b.c.simple -> simple .
		if ( lastPeriod != -1 )
		{
			result = name.substring( lastPeriod + 1 );
		}
		else
		{
			result = name;
		}
									// Return trimmed class name.
		return result;
	}

	/** Gets interfaces for a class.
	 *
	 *	@param 	infoClass 	Class for which interfaces are wanted.
	 *
	 *	@return				String array of interfaces.
	 *
	 */

	public static String[] getInterfaces( Class infoClass )
	{
		String[] result = null;
									// Get list of interfaces implemented
									// by class.

		Class [] classes = infoClass.getInterfaces();

		if ( classes.length > 0 )
		{
			result = new String[ classes.length ];

			for ( int i = 0; i < classes.length; i++ )
			{
				result[ i ] = trimClassName( classes[ i ].getName() );
       		}
		}
									// Return string array of interface names,
									// or null if no interfaces implemented.
		return result;
    }

	/** Gets constructors for a class.
	 *
	 *	@param 	infoClass 	Class for which constructors are wanted.
	 *
	 *	@return				String array of constructors.
	 */

	public static String[] getConstructors( Class infoClass )
	{
		String[] result = null;
		Constructor[] constructors = null;

									// Get list of constructors.
		try
		{
			constructors = infoClass.getDeclaredConstructors();
		}
		catch ( SecurityException e )
		{
			return result;
		}
									// If no constructors, return null.

		if ( ( constructors == null ) || ( constructors.length == 0 ) ) return result;

		result = new String[ constructors.length ];

									// The "result" entries will be an array
									// of constructors with modifiers and
									// parameters.

		for ( int i = 0; i < constructors.length; i++ )
		{
			Constructor constructor = constructors[ i ];

			int modifiers = constructor.getModifiers();

			result[ i ] = "";
									// Add modifiers to constructor name.
			if ( modifiers > 0 )
			{
				result[ i ] = trimClassName( Modifier.toString( modifiers ) ) + " ";
			}
									// Trim array declarations.

			result[ i ] = result[ i ] + trimClassName( infoClass.getName() ) + "(";

									// Add parameter types to constructor name.

			Class[] paramTypes = constructor.getParameterTypes();

			for ( int j = 0; j < paramTypes.length; j++ )
			{
				if ( j > 0 )
				{
					result[ i ] = result[ i ] + ", ";
				}

				result[ i ] = result[ i ] +
					trimClassName( unmangleJavaName( paramTypes[ j ].getName() ) );
			}

			result[ i ] = result[ i ] + ")";

                                    // Add exception types to constructor name.

			Class[] exceptionTypes = constructor.getExceptionTypes();

			for ( int j = 0; j < exceptionTypes.length; j++ )
			{
				if ( j == 0 )
				{
					result[ i ] = result[ i ] + " throws";
				}
				else
				{
					result[ i ] = result[ i ] + ", ";
				}

				result[ i ] = result[ i ] + " " +
					trimClassName( unmangleJavaName( exceptionTypes[ j ].getName() ) );
			}
        }
									// Return array of constructor names and associated
									// information formatted as strings.

        return result;
	}

	/** Get methods for a class.
	 *
	 *	@param 	infoClass 	Class for which methods are wanted.
	 *
	 *	@return				String array of methods.
	 *
	 */

	public static String[] getMethods( Class infoClass )
	{
		String[] result		= null;
		Method[] methods	= null;

									// Get list of all methods, both public
									// and private.
		try
		{
			methods = infoClass.getDeclaredMethods();
		}
		catch ( SecurityException e )
		{
			return result;
		}
									// Return null if no methods.

		if ( ( methods == null ) || ( methods.length == 0 ) ) return result;

                                    // Sort the method names.

		Comparator comparator = new Comparator()
		{
			public int compare ( Object o1, Object o2 )
			{
				Method m1 = (Method)o1;
				Method m2 = (Method)o2;

				return m1.getName().compareTo( m2.getName() );
			}
		};

		Arrays.sort( methods , comparator );

		result = new String[ methods.length ];

									// The "result" entries will be an array
									// of methods with return type, modifiers,
									// and parameters, sorted in ascending order
									// by method name.

		for ( int i = 0; i < methods.length; i++ )
		{
			Method thisMethod = methods[ i ];

			Class retType = thisMethod.getReturnType();

			int modifiers = thisMethod.getModifiers();

			result[ i ] = "";
									// Add modifiers to method name.

			if ( modifiers != 0 )
			{
				result[ i ] = trimClassName( unmangleJavaName( Modifier.toString( modifiers ) ) + " " );
			}
									// Add return type to method name.

			result[ i ] = 	result[ i ] +
							trimClassName( unmangleJavaName( retType.getName() ) ) +
							" " + trimClassName( thisMethod.getName() ) + "(";

									// Append parameters to method name.

			Class[] paramTypes = thisMethod.getParameterTypes();

			for ( int j = 0; j < paramTypes.length; j++ )
			{
				if ( j > 0 )
				{
					result[ i ] = result[ i ] + ", ";
				}

				result[ i ] = 	result[ i ] +
								trimClassName( unmangleJavaName( paramTypes[ j ].getName() ) );
			}

			result[ i ] = result[ i ] + ")";

									// Append exceptions thrown to method name.

			Class[] exceptionTypes = thisMethod.getExceptionTypes();

			for ( int j = 0; j < exceptionTypes.length; j++ )
			{
				if ( j == 0 )
				{
					result[ i ] = result[ i ] + " throws";
				}
				else
				{
					result[ i ] = result[ i ] + ", ";
				}

				result[ i ] = result[ i ] + " " +
					trimClassName( unmangleJavaName( exceptionTypes[ j ].getName() ) );
			}
		}
									// Return array of method names and associated
									// information formatted as strings.
		return result;
	}

	/** Get fields for a class.
	 *
	 *	@param 	infoClass 	Class for which fields are wanted.
	 *
	 *	@return				String array of field names with modifiers.
	 *
	 */

	public static String[] getFields( Class infoClass )
	{
		String[] result	= null;
		Field[] fields	= null;
									// Get list of field names, both public
									// and private.
		try
		{
			fields = infoClass.getDeclaredFields();
		}
		catch ( SecurityException e )
		{
			return result;
		}
									// Return null result if no field names.

		if ( ( fields == null ) || ( fields.length == 0 ) ) return result;

                                    // Sort field names.

		Comparator comparator = new Comparator()
		{
			public int compare ( Object o1, Object o2 )
			{
				Field f1 = (Field)o1;
				Field f2 = (Field)o2;

				return f1.getName().compareTo( f2.getName() );
			}
		};

		Arrays.sort( fields , comparator );

									// Append any modifiers to each field name.
									// The "result" entries will be an array
									// of field names with modifiers attached,
									// sorted in ascending order by field name.

		result = new String[ fields.length ];

		for ( int i = 0; i < fields.length; i++ )
		{
			Field thisField = fields[ i ];

			Class type = thisField.getType();

			result[ i ] = "";

			int modifiers = thisField.getModifiers();

			if( modifiers > 0 )
			{
				result[ i ] =
					trimClassName( unmangleJavaName( Modifier.toString( modifiers ) ) ) + " ";
			}

			result[ i ] = result[ i ] +
				trimClassName( unmangleJavaName( type.getName() ) ) +
				" " + trimClassName( unmangleJavaName( thisField.getName() ) );
		}
									// Return string array of field names with
									// modifiers appended.
		return result;
	}

	/** Get signers for a class.
	 *
	 *	@param 	infoClass 	Class for which signers are wanted.
	 *
	 *	@return				String array of signers.
	 *
	 */

	public static String[] getSigners( Class infoClass )
	{
		String result[]		= null;
		Object[] signers	= null;

									// Get list of signers.

		signers = infoClass.getSigners();

									// Return null result if no signers.

		if ( ( signers == null ) || ( signers.length == 0 ) ) return result;

									// Sort signers.

		Comparator comparator = new Comparator()
		{
			public int compare ( Object o1, Object o2 )
			{
				return o1.toString().compareTo( o2.toString() );
			}
		};

		Arrays.sort( signers , comparator );

		result = new String[ signers.length ];

		for ( int i = 0; i < signers.length; i++ )
		{
			Object signer = signers[ i ];
			result[ i ] = signer.toString();
		}
                                    // The "result" returns an array
                                    // of the signers.
		return result;
	}

	/** Get classes for a class.
	 *
	 *	@param 	infoClass 	Class for which classes are wanted.
	 *
	 *	@return				String array of classes.
	 *
	 */

	public static String[] getClasses( Class infoClass )
	{
		String result[]		= null;
		Object[] classes	= null;

									// Get list of internal classes, both
									// public and private.

		classes = infoClass.getDeclaredClasses();

									// Return null result if no classes.

		if ( ( classes == null ) || ( classes.length == 0 ) ) return result;

									// Sort classes in ascending order by class name.

		Comparator comparator = new Comparator()
		{
			public int compare ( Object o1, Object o2 )
			{
				return o1.toString().compareTo( o2.toString() );
			}
		};

		Arrays.sort( classes , comparator );

		result = new String[ classes.length ];

									// Create "result" array containing list
									// of class names.

		for ( int i = 0; i < classes.length; i++ )
		{
			Object aClass = classes[ i ];

									// Throw away the leading "class" etc.
									// modifier.

			result[ i ] = trimClassName( aClass.toString() );

			int firstBlank = result[ i ].indexOf( ' ' );

			if ( firstBlank != -1 )
			{
				result[ i ] = result[ i ].substring( firstBlank + 1 );
			}
		}
									// Return string array of class names.
		return result;
	}

	/** Check if class implements specified interface.
	 *
	 *	@param 	className 		Class name.
	 *
	 *	@param 	interfaceName 	Interface name.
	 *
	 *	@return					True if class implements interface.
	 *
	 */

	public static boolean classImplements( String className , String interfaceName )
		throws ClassNotFoundException
	{
		boolean result	= false;

		Class infoClass	= Class.forName( className );

									// Get list of interfaces implemented by class.

		String[] interfaces = getInterfaces( infoClass );

                                    // See if the requested interface appears
                                    // in the list.
		if ( interfaces != null )
		{
			for ( int i = 0; i < interfaces.length; i++ )
			{
				result = result || interfaces[ i ].equals( interfaceName );
			}
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

