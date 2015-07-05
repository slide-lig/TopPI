package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.util.*;
import java.util.regex.*;

/**	Filters a String collection using a regular expression.
 */

public class RegExpCollectionFilter
{
	/**	Source pattern string. */

	protected String sourcePattern;

	/**	Compiled source pattern matcher. */

	protected Matcher sourcePatternMatcher;

	/**	Create a filter definition.
	 *
	 *	@param	sourcePattern		Source pattern string as a
	 *								regular expression.
	 */

	public RegExpCollectionFilter
	(
		String sourcePattern
	)
	{
		this.sourcePattern			= sourcePattern;

		this.sourcePatternMatcher	=
			Pattern.compile( sourcePattern ).matcher( "" );
	}

	/**	Filter a string collection using the pattern..
	 *
	 *	@param	collection	String collection to filter.
	 *
	 *	@return				Filtered collection with string matching pattern
	 *						removed.
	 */

	public Collection filter( Collection collection )
	{
		Collection result	= null;

		try
		{
			result	= (Collection)collection.getClass().newInstance();

			result.addAll( collection );

			String s;

			for	(	Iterator iterator = result.iterator() ;
					iterator.hasNext() ;
				)
			{
				s	= iterator.next().toString();

				if ( !sourcePatternMatcher.reset( s ).matches() )
				{
					iterator.remove();
				}
			}
		}
		catch ( Exception ignored )
		{
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


