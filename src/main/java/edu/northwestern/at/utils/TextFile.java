package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import javax.swing.*;
import java.awt.Component;
import java.io.*;
import java.util.*;

/** TextFile reads a text file into an array of strings.
 */

public class TextFile
{
	/** The text file.  May be null if file read from a stream. */

	protected File textFile				= null;

	/**	The text file encoding.  Defaults to ISO Latin 1. */

	protected String textFileEncoding	= "8859_1";

	/** The text of the data file, split into lines. */

	protected String[] textFileLines	= null;

	/**	True if file loaded OK. */

	protected boolean textFileLoaded	= false;

	/** Create text file object from file with specified encoding.
	 *
	 *	@param	textFile	Text file.
	 *	@param	encoding	Text file encoding (utf-8, 8859_1, etc.).
	 */

	public TextFile( File textFile , String encoding )
	{
		this.textFile			= textFile;

		String safeEncoding		= ( encoding == null ) ? "" : encoding;
        safeEncoding			= safeEncoding.trim();

		if ( safeEncoding.length() > 0 )
		{
			this.textFileEncoding	= safeEncoding;
        }

		openFile( textFile );
	}

	/** Create text file object from named file with specified encoding.
	 *
	 *	@param	textFileName	The data file name.
	 *	@param	encoding		Text file encoding (utf-8, 8859_1, etc.).
	 */

	public TextFile( String textFileName , String encoding )
	{
		this( new File( textFileName ) , encoding );
	}

	/** Create text file object from named file.
	 *
	 *	@param	textFileName	The data file name.
	 */

	public TextFile( String textFileName )
	{
		this( textFileName , null );
	}

	/** Create text file object from local file.
	 *
	 *	@param	textFile	The data file.
	 */

	public TextFile( File textFile )
	{
		this( textFile , null );
	}

	/**	Create data file object from input stream.
	 *
	 *	@param	inputStream		The input stream for the data file.
	 *	@param	encoding		Text file encoding (utf-8, 8859_1, etc.).
	 */

	public TextFile( InputStream inputStream , String encoding )
	{
		this.textFile	= null;

		String safeEncoding		= ( encoding == null ) ? "" : encoding;
        safeEncoding			= safeEncoding.trim();

		if ( safeEncoding.length() > 0 )
		{
			this.textFileEncoding	= safeEncoding;
        }

		openInputStream( inputStream );
	}

	/**	Create data file object from input stream.
	 *
	 *	@param	inputStream		The input stream for the data file.
	 */

	public TextFile( InputStream inputStream )
	{
		this( inputStream , null );
	}

	/**	Read local file into array of strings.
	 *
	 *	@param	inputFile	The input file.  The file is opened
	 *						using the urf-8 character set.
	 */

	protected void openFile( File inputFile )
	{
		try
		{
			openInputStream( new FileInputStream( inputFile ) );

			textFileLoaded	= true;
		}
		catch ( FileNotFoundException e )
		{
		}
	}

	/**	Read stream into array of strings.
	 *
	 *	@param	inputStream		The InputStream for the file.
	 */

	protected void openInputStream( InputStream inputStream )
	{
		String textLine;
								// Collect input lines in an array list.

		ArrayList lines					= new ArrayList();
		BufferedReader bufferedReader	= null;

		try
		{
			bufferedReader	=
				new BufferedReader
				(
					new InputStreamReader( inputStream , textFileEncoding )
				);

			while ( ( textLine = bufferedReader.readLine() ) != null )
			{
				lines.add( textLine );
			}

			textFileLoaded	= true;
		}
		catch ( IOException e )
		{
		}
		finally
		{
			try
			{
				if ( bufferedReader != null ) bufferedReader.close();
			}
			catch ( Exception e )
			{
			}
		}
								// Convert array list to array of strings.

		textFileLines	= new String[ lines.size() ];

		for ( int i = 0 ; i < lines.size() ; i++ )
		{
			textFileLines[ i ]	= (String)lines.get( i );
		}
	}

	/**	Return number of lines in the data file.
	 *
	 *	@return		Number of lines in the file.
	 */

	public int size()
	{
		return	textFileLines.length;
	}

	/** Did text load OK?
	 *
	 *	@return		true if text file loaded OK.
	 */

	public boolean textLoaded()
	{
		return textFileLoaded;
	}

	/**	Return file contents as a string array.
	 *
	 *	@return		File contents as a string array.
	 */

	public String[] toArray()
	{
		return textFileLines;
	}

	/**	Return file contents as a string.
	 *
	 *	@return		File contents as a string.
	 *
	 *	<p>
	 *	Each line of the data file is separated by a \n character
	 *	in the returned string.
	 *	</p>
	 */

	public String toString()
	{
		StringBuffer sb	= new StringBuffer( 32768 );

		for ( int i = 0 ; i < textFileLines.length ; i++ )
		{
			sb.append( textFileLines[ i ] );
			sb.append( "\n" );
		}

		return sb.toString();
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

