package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.StringTokenizer;
import java.util.Vector;

/** Retrieves text from a global string resource bundle.
 *
 *	<p>
 *	Many items in the utils classes require textual information.
 *	This class provides a simple mechanism for consolidating the
 *	location of the text via a resource bundle.  Wherever
 *	a text string would normally be used in the code,
 *	a reference to the "get" methods of this class should be used instead.
 *	</p>
 *
 *	<p>
 *	For example, when creating a button, instead of writing<br />
 *	<code>
 *	JButton button = new JButton( "bogus" );
 *	</code>
 *	write<br />
 *	<code>
 *	JButton button = new JButton( Resources.get( "bogus" , "bogus" ) );
 *	</code>
 *	instead.  The first string is the name of the resource, and
 *	the second is a default value, typically in English.
 *	</p>
 *
 *	<p>
 *	The file "utils.properties" contains strings in
 *	English and is found in the resources/ subdirectory underneath
 *	this class.  You may add corresponding properties files for
 *	other languages, and call one of the "setLocale" methods here
 *	to use these other properties files.  For example, to define
 *	French strings, create the file resources/utils_fr.properties
 *	and call:
 *	</p>
 *
 *	<p>
 *	<code>Resources.setLocale( "fr" );</code>
 *	</p>
 *
 *	<p>
 *	to use your French strings.
 *	</p>
 *
 *	<p>
 *	If you define an alternative properties file for another
 *	language, you should provide translation for all the strings
 *	defined in en.properties .  Undefined strings will revert the to
 *	default values in English declared in the "get" method calls.
 *	</p>
 */

public class Resources
{
	/**	Resource base name. */

	protected static String resourceName	= "utils";

	/** String resource bundle.  */

	protected static ResourceBundle resourceBundle = null;

	/**	Default locale. */

	protected static Locale defaultLocale	= new Locale( "en" , "US" );

	/** Set the resource bundle to use based upon the locale.
	 *
	 *	@param	locale		Locate specifying language and country.
	 */

	public static void setLocale( Locale locale )
	{
		if ( locale != null )
		{
			try
			{
				resourceBundle =
					ResourceBundle.getBundle(
						Resources.class.getName() + "." + resourceName ,
						locale );
			}
			catch ( MissingResourceException e1 )
			{
				try
				{
					resourceBundle =
						ResourceBundle.getBundle(
							Resources.class.getName() + "." + resourceName ,
							defaultLocale );
				}
				catch( MissingResourceException e2 )
				{
				}
			}
		}
		else
		{
			try
			{
				resourceBundle =
					ResourceBundle.getBundle(
						Resources.class.getName() + "." + resourceName ,
						defaultLocale );
			}
			catch( MissingResourceException e )
			{
			}
		}
	}

	/** Set the translation language using the language name and country.
	 *
	 *	@param	language	The java style language name.
	 *						Example:  French is "fr".
	 *
	 *	@param	country		The java style country name.
	 *						Example:  France is "FR".
	 */

	public static void setLocale( String language , String country )
	{
		if ( ( language != null ) && ( country != null ) )
		{
			setLocale( new Locale( language , country ) );
		}
	}

	/** Set the translation language using the language name.
	 *
	 *	@param	language	The java style language name.
	 *						Example:  French is "fr".
	 */

	public static void setLocale( String language )
	{
		if ( language != null )
		{
			setLocale( new Locale( language ) );
		}
	}

	/**	Get string from ResourceBundle.  If no string is found, a default
	 *  string is used.
	 *
	 *	@param	resourceName	Name of resource to retrieve.
	 *	@param	defaultValue	Default value for resource.
	 *
	 *	@return   				String value from resource bundle if
	 *							resourceName found there, otherwise
	 *							defaultValue.
	 *
	 *	<p>
	 *	Underline "_" characters are replaced by spaces.
	 *	</p>
	 */

	public static String get
	(
		String resourceName ,
		String defaultValue
	)
	{
		String result;

		try
		{
			result	= resourceBundle.getString( resourceName );
		}
		catch ( MissingResourceException e )
		{
			result	= defaultValue;
		}

		result	= result.replace( '_' , ' ' );

		return result;
	}

	/**	Get string from ResourceBundle.  If no string is found, an empty
	 *  string is returned.
	 *
	 *	@param	resourceName	Name of resource to retrieve.
	 *
	 *	@return   				String value from resource bundle if
	 *							resourceName found there, otherwise
	 *							empty string.
	 *
	 *	<p>
	 *	Underline "_" characters are replaced by spaces.
	 *	</p>
	 */

	public static String get( String resourceName )
	{
		String result;

		try
		{
			result	= resourceBundle.getString( resourceName );
		}
		catch ( MissingResourceException e )
		{
			result	= "";
		}

		result	= result.replace( '_' , ' ' );

		return result;
	}

	/**	Parse ResourceBundle for a String array.
	 *
	 *	@param	resourceName	Name of resource.
	 *	@param  defaults		Array of default string values.
	 *	@return					Array of strings if resource name found
	 *							in resources, otherwise default values.
	 */

	public static String[] gets
	(
		String resourceName,
		String[] defaults
	)
	{
		String[] result;

		try
		{
			result = tokenize( resourceBundle.getString( resourceName ) );
		}
		catch ( MissingResourceException e )
		{
			result	= defaults;
		}

		return result;
	}

	/**	Split string into a series of substrings on whitespace boundries.
	 *
	 *	@param	input	Input string.
	 *	@return			The array of strings after splitting input.
	 *
	 *	<p>
	 *	This is useful for retrieving an array of strings from the
	 *	resource file.  Underline "_" characters are replaced by spaces.
	 *	</p>
	 */

	public static String[] tokenize( String input )
	{
		Vector v			= new Vector();
		StringTokenizer t	= new StringTokenizer( input );
		String result[];
		String s;

		while ( t.hasMoreTokens() )
		{
			v.addElement( t.nextToken() );
		}

		result	= new String[ v.size() ];

		for ( int i = 0 ; i < result.length ; i++ )
		{
			s			= (String)v.elementAt( i );
			result[ i ]	= s.replace( '_' , ' ' );
		}

		return result;
	}

	/** Don't allow instantiation, do allow overrides. */

	protected Resources()
	{
	}
								// Load the default resource file.
	static
	{
		try
		{
			resourceBundle	=
				ResourceBundle.getBundle(
					Resources.class.getName() + "." + resourceName ,
					defaultLocale );
		}
		catch ( MissingResourceException e )
		{
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

