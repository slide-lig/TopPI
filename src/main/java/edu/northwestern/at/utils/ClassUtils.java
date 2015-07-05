package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.util.*;
import java.text.*;

/**	Class utilities.
 *
 *	<p>
 *	This static class provides various utility methods for manipulating
 *	class names.
 *	</p>
 */

public class ClassUtils
{
	/**	Extracts the unqualified class name from a fully qualified
	 *	class name.
	 *
	 *	@param	name	The fully qualified class name.
	 *
	 *	@return			The unqualified class name.
	 */

	public static String unqualifiedName( String name )
	{
		int index	= name.lastIndexOf( '.' );

		return	name.substring( index + 1 );
	}

	/**	Extracts the package name from a fully qualified class name.
	 *
	 *	@param	name	The fully qualified class name.
	 *
	 *	@return			The package name.
	 */

	public static String packageName( String name )
	{
		int index	= name.lastIndexOf( '.' );

		return name.substring( 0 , index );
	}

	/** Don't allow instantiation, do allow overrides. */

	protected ClassUtils()
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

