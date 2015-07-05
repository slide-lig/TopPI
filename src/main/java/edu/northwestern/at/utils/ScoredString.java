package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.io.Serializable;
import java.util.*;
import edu.northwestern.at.utils.*;

/**	Associates a string with a score.
 */

public class ScoredString implements Comparable, Serializable
{
	/**	The string. */

	protected String string;

	/**	The string score. */

	protected double score;

	/**	Create scored string.
	 */

	public ScoredString()
	{
		this.string	= "";
		this.score	= 0.0D;
	}

	/**	Create scored string.
	 *
	 *	@param	string	String.
	 *	@param	score	Score.
	 */

	public ScoredString( String string , double score )
	{
		this.string	= string;
		this.score	= score;
	}

	/**	Get string.
	 *
	 *	@return		The string.
	 */

	public String getString()
	{
		return string;
	}

	/**	Set string.
	 *
	 *	@param	string	The string.
	 */

	public void putString( String string )
	{
		this.string	= string;
	}

	/**	Get score.
	 *
	 *	@return		The score.
	 */

	public double getScore()
	{
		return score;
	}

	/**	Set score.
	 *
	 *	@param	score	The score.
	 */

	public void setScore( double score )
	{
		this.score	= score;
	}

	/**	Generate displayable string.
	 *
	 *	@return		String followed by score in parentheses.
	 */

	public String toString()
	{
		return string + " (" + score + ")";
	}

	/**	Check if another object is equal to this one.
	 *
	 *	@param	other	Other object to test for equality.
	 *
	 *	@return			true if other object is equal to this one.
	 */

	public boolean equals( Object other )
	{
		boolean result	= false;

		if ( other instanceof ScoredString )
		{
			ScoredString otherScoredString	= (ScoredString)other;

			result	=
				( string.equals( otherScoredString.getString() ) ) &&
				( score == otherScoredString.getScore() );
		}

		return result;
	}

 	/**	Compare this scored string with another.
 	 *
 	 *	@param	other	The other scored string
 	 *
	 *	@return			< 0 if this scored string is less than the other,
	 *					= 0 if the two scored strings are equal,
	 *					> 0 if this scored string is greater than the other.
 	 */

	public int compareTo( Object other )
	{
		int result	= 0;

		if ( ( other == null ) ||
			!( other instanceof ScoredString ) )
		{
			result	= Integer.MIN_VALUE;
		}
		else
		{
			ScoredString otherScoredString	= (ScoredString)other;

			result	= Compare.compare( score , otherScoredString.getScore() );

			if ( result == 0 )
			{
				result	=
					-Compare.compare( string , otherScoredString.getString() );
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


