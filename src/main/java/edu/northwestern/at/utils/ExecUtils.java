package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.io.*;
import java.util.*;

/** Execute system commands.
 */

public class ExecUtils
{
	/** Execute file/command and wait for command to complete.
	 *
	 *	@param	command		The command or file name to execute.
	 *	@param	doWait		True to wait for the command to return.
	 *
	 *	@return				An array list of the output written
	 *						by the program to standard output, if doWait
	 *						is true.
	 *	<p>
	 *	Examples:
	 *	</p>
	 *
	 *	<p>
	 *	Windows only directory listing:
	 *	</p>
	 *	<code>
	 *		ArrayList outputLinesList = execAndWait( "dir" , true );
	 *	</code>
	 *
	 *	<p>
	 *	Directory listing for MacOSX and similar Unixen:
	 *	</p>
	 *
	 *	<code>
	 *		ArrayList outputLinesList = execAndWait( "ls -la" , true );
	 *	</code>
	 *
	 *	<p>
	 *	Open Acrobat file in Acrobat reader, if installed (Windows and MacOSX):
	 *	</p>
	 *
	 *	<code>
	 *		ArrayList outputLinesList = execAndWait( "mydoc.pdf" , false );
	 *	</code>
	 *
	 *	<p>
	 *	Note:	On some Unixen and possibly MacOSX, you may need to
	 *			add a path specifier to get the file "executed."
	 *			E.g., if the document is in the current directory:
	 *	</p>
	 *	<code>
	 *		ArrayList outputLinesList = execAndWait( "./mydoc.pdf" , false );
	 *	</code>
	 *
	 *	<p>
	 *	When running a file that causes the associated application
	 *	to start, and doWait is true, execAndWait may not return until
	 *	that associated application is closed.  You should invoke execAndWait
	 *	on a separate thread if you want the execute the command and allow
	 *	your program to continue executing concurrently as well.
	 *	</p>
	 */

	public static ArrayList execAndWait( String command , boolean doWait )
	{
								// Allocate array list to hold any output of
								// program executed.

		ArrayList result = new ArrayList();

								// Get runtime so we can execute the program.

		Runtime runtime = Runtime.getRuntime();

                         		// Java exec returns a process.  We use
                         		// that to get the output of the command,
                         		// if any, and to wait for the command to finish.

		Process process = null;

                                // Start the program.  We choose the command
                                // processor differently based upon what system
                                // we are running under.
		try
		{
								// Use "open" under Mac OSX.
			if ( Env.MACOSX )
			{
				process = runtime.exec( new String[]{ "open", command } );
			}
								// Use command.com or cmd.exe under Windows.

			else if ( Env.WINDOWSOS )
			{
				String shell = "cmd.exe";

				if ( Env.OSNAME.indexOf( "windows 9" ) > -1 )
				    shell = "command.exe";
				else if ( Env.OSNAME.indexOf( "windows ME" ) > -1 )
				    shell = "command.exe";

				process =
					runtime.exec( new String[]{ shell , " /c " + command } );
			}
								// Just try executing the command directly
								// on other systems.  This needs work.
			else
			{
				process = runtime.exec( command );
			}
		}
		catch ( IOException e )
		{
		}
								// If the command process started successfully,
								// we should wait for its output to finish
								// to avoid weird process hangs.  However, if
								// "doWait" is false, don't bother to wait.

		if ( ( process != null ) && doWait )
		{
            					// Create a buffered reader input stream
            					// to retrieve the command process output.

			InputStream inputStream = process.getInputStream();

			BufferedReader bufferedReader =
				new BufferedReader( new InputStreamReader( inputStream ) );

								// Read and store output lines from the
								// command process.
			String s = "";

			try
			{
				while ( ( s = bufferedReader.readLine() ) != null )
					result.add( s );
			}
			catch ( IOException e )
			{
			}
								// Wait for command to finish.
			try
			{
				process.waitFor();
			}
			catch ( InterruptedException e )
			{
				Thread.currentThread().interrupt();
			}
								// Close stream.
			try
			{
				bufferedReader.close();
			}
			catch ( IOException e )
			{
			}
		}
								// Return process output lines.
		return result;
	}

	/** Don't allow instantiation, do allow overrides. */

	protected ExecUtils()
	{
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

