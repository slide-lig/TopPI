package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.util.*;
import java.io.*;
import java.text.*;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;

/** Utilities for formatting dates and times.
 */

public class DateTimeUtils
{
	/** Seconds per day.
	 */

	public static final int SECONDS_PER_DAY = 86400;

	/** Seconds per hour.
	 */

	public static final int SECONDS_PER_HOUR = 3600;

	/** Seconds per minute.
	 */

	public static final int SECONDS_PER_MINUTE = 60;

	/**	Date formatter for dates and times in format e.g. 02/06/02 10:32 AM. */

	static private final SimpleDateFormat dateFormatter1 =
		new SimpleDateFormat("MM/dd/yy hh:mm aa");

	/**	Formats a date and time as (for example) "02/06/02 10:32 AM".
	 *
	 *	@param	date		The date and time.
	 *
	 *	@return				The formatted date and time.
	 */

	public static String formatDate (Date date) {
		return dateFormatter1.format(date);
	}

	/**	Date formatter for dates in format e.g. 2/6/2002. */

	static private final SimpleDateFormat dateFormatter2 =
		new SimpleDateFormat("M/d/yyyy");

	/**	Formats a date as (for example) "2/6/2002".
	 *
	 *	@param	date		The date.
	 *
	 *	@return				The formatted date.
	 */

	public static String formatDateNoTime (Date date) {
		return dateFormatter2.format(date);
	}

	/**	Date formatter for dates in format e.g. 02/06/02. */

	static private final SimpleDateFormat dateFormatter3 =
		new SimpleDateFormat("MM/dd/yy");

	/**	Timer formatter for dates in format hh:mm aa. */

	static private final SimpleDateFormat timeFormatter =
		new SimpleDateFormat( "hh:mm aa" );

	/**	Timer formatter for dates in format hh:mm . */

	static private final SimpleDateFormat timeFormatter2 =
		new SimpleDateFormat( "hh:mm" );

	/**	Timer formatter for dates in format hh:mm . */

	static private final SimpleDateFormat timeFormatter3 =
		new SimpleDateFormat( "hh:mm:ss" );

	/**	Formats a time as (for example) "10:32 AM".
	 *
	 *	@param	date		The time.
	 *
	 *	@return				The formatted time.
	 */

	public static String formatTime (Date date) {
		return timeFormatter.format(date);
	}

	/**	Parses a date.
	 *
	 *	@param	str			String.
	 *
	 *	@return				The date, or null if syntax error.
	 */

	static public Date parseDate (String str) {
		ParsePosition pos = new ParsePosition(0);
		Date result = dateFormatter3.parse(str, pos);
		if (result != null && pos.getIndex() < str.length()) result = null;
		return result;
	}

	/**	Parses time of day in hh:mm aa format.
	 *
	 *	@param	str			String.
	 *
	 *	@return				The time, or null if syntax error.
	 *
	 *	<p>
	 *	First attempts to parse the time in "hh:mm aa" format.
	 *	If that fails, tries "hh:mm".  The latter allows for
	 *	inputting the time on a 24 clock basis.
	 *	</p>
	 */

	static public Date parseTime( String str )
	{
		ParsePosition pos = new ParsePosition( 0 );

		Date result = timeFormatter.parse(str, pos);

		if ( ( result != null ) && ( pos.getIndex() < str.length() ) )
			result = null;

		if ( result == null )
		{
			pos = new ParsePosition( 0 );

			result = timeFormatter2.parse( str , pos );

			if ( ( result != null ) && ( pos.getIndex() < str.length() ) )
				result = null;
		}

		return result;
	}

	/**	Formats a date and time as (for example) "on 02/06/02 at 10:32 AM".
	 *
	 *	@param	date		The date and time.
	 *
	 *	@return				The formatted date and time.
	 */

	public static String formatDateOnAt (Date date) {
		String str = formatDate(date);
		int i = str.indexOf(' ');
		return "on " + str.substring(0, i) + " at " + str.substring(i+1);
	}

	/**	Format a time duration as "hh:mm:ss"
	 *
	 *	@param	milliseconds	Time duration in milliseconds
	 *
	 *	@return					Formated time duration.
	 */

	public static String formatDuration (long milliseconds) {
		long seconds = milliseconds/1000;
		long hours = seconds/3600;
		seconds -= hours*3600;
		long minutes = seconds/60;
		seconds -= minutes*60;
		return (hours < 10 ? "0" : "") + hours + ":" +
			(minutes < 10 ? "0" : "") + minutes + ":" +
			(seconds < 10? "0" : "") + seconds;
	}

	/**	Format a wordy time duration (e.g., "5 days, 3 hours, 23 minutes").
	 *
	 *	@param	milliseconds	Time duration in milliseconds
	 *
	 *	@return					Formated time duration.
	 */

	public static String formatWordyDuration (long milliseconds) {
		long seconds = milliseconds/1000;
		long days = seconds/86400;
		seconds -= days*86400;
		long hours = seconds/3600;
		seconds -= hours*3600;
		long minutes = seconds/60;
		seconds -= minutes*60;
		StringBuffer buf = new StringBuffer();
		if (days > 0) buf.append(days +
			(days == 1 ? " day, " : " days, "));
		if (days > 0 || hours > 0) buf.append(hours +
			(hours == 1 ? " hour, " : " hours, "));
		buf.append(minutes +
			(minutes == 1 ? " minute" : " minutes"));
		return buf.toString();
	}

	/**	Clears the time in a Gregorian calendar date.
	 *
	 *	@param		date	The date.
	 */

	public static void clearTime( GregorianCalendar date )
	{
		date.set( Calendar.HOUR_OF_DAY , 0 );
		date.set( Calendar.MINUTE , 0 );
		date.set( Calendar.SECOND , 0 );
		date.set( Calendar.MILLISECOND , 0 );
	}

	/**	Adds specified time of day to a date.
	 *
	 *	@param		date	The date.
	 *	@param		time	The time.
	 *
	 *	@return		Date field with combined (date, time) set.
	 */

	public static Date addTimeToDate( Date date , Date time )
	{
		String dateAndTime =
			formatDateNoTime( date ) + " " + formatTime( time );

		ParsePosition pos = new ParsePosition( 0 );
		Date result = dateFormatter1.parse( dateAndTime , pos );

		if (	( result != null ) &&
				( pos.getIndex() < dateAndTime.length() ) ) result = null;

		return result;
	}

	/**	Date formatter for dates and times in HTTP protocol format e.g.
	 *	Wed, 13 Aug 2003 22:03:00 GMT
	 */

	static private final SimpleDateFormat httpDateFormatter =
		new SimpleDateFormat( "EEE, dd MMM yyyy hh:mm:ss 'GMT'" );

	/**	Formats a date and time in HTTP protocol format.
	 *
	 *	@param	date		The date and time.
	 *
	 *	@return				The formatted date and time.
	 */

	public static String formatHTTPDate( Date date )
	{
		return httpDateFormatter.format( date );
	}

	/** Create a timer value from days, hours, minutes, seconds.
	 *
	 *	@param	days	Days.
	 *	@param	hours	Hours.
	 *	@param	minutes	Minutes.
	 *	@param	seconds	Seconds.
	 *
	 *	@return			The corresponding long timer value in milliseconds.
	 *
	 *	<p>
	 *	Note: The parameters are assumed to contain legal values.
	 *	</p>
	 */

	public static long makeTimerValue
	(
		long days,
		long hours,
		long minutes,
		long seconds
	)
	{
		return	(	seconds +
					minutes * SECONDS_PER_MINUTE +
					hours * SECONDS_PER_HOUR +
					days * SECONDS_PER_DAY ) * 1000L;
	}

	/** Formats a timer value as "days hh:mm:ss".
	 *
	 *	@param	timerValue		The timer value in milliseconds.
	 *	@param	showZeroFields	True to show explicit zero for
	 *							days when days value is zero, and/or
	 *							explicit 00:00:00 when hh:mm:ss
	 *							value is zero.
	 *
	 *	@return					The formatted timer value.
	 */

	public static String formatTimer
	(
		long timerValue , boolean showZeroFields
	)
	{
		long seconds, days, hours, minutes, sum;

		seconds		= timerValue / 1000;

		days		= seconds / SECONDS_PER_DAY;
		seconds		= seconds - ( days * SECONDS_PER_DAY );

		hours		= seconds / SECONDS_PER_HOUR;
		seconds		= seconds - ( hours * SECONDS_PER_HOUR );

		minutes		= seconds / SECONDS_PER_MINUTE;
		seconds 	= seconds - ( minutes * SECONDS_PER_MINUTE );

		sum			= hours + minutes + seconds;

		String result = "";

		if ( ( ( days == 0 ) && showZeroFields ) || ( days != 0 ) )
		{
			result = StringUtils.longToString( days ) + " ";
		}

		if ( ( ( sum == 0 ) && showZeroFields ) || ( sum != 0 ) )
		{
			result = result +
				StringUtils.longToStringWithZeroFill( hours , 2 ) + ":" +
				StringUtils.longToStringWithZeroFill( minutes , 2 ) + ":" +
				StringUtils.longToStringWithZeroFill( seconds , 2 );
		}

		if ( result.length() == 0 ) result = "0";

		return result;
	}

	/** Formats a timer value in readable form.
	 *
	 *	@param	timerValue		The timer value in milliseconds.
	 *
	 *	@return					The formatted timer value.
	 *
	 *	<p>
	 *	The readable format is: n day(s), h hours, m minutes, s seconds.
	 *	</p>
	 */

	public static String formatReadableTimer( long timerValue )
	{
		long seconds, days, hours, minutes, sum;

		seconds		= timerValue / 1000;

		days		= seconds / SECONDS_PER_DAY;
		seconds		= seconds - ( days * SECONDS_PER_DAY );

		hours		= seconds / SECONDS_PER_HOUR;
		seconds		= seconds - ( hours * SECONDS_PER_HOUR );

		minutes		= seconds / SECONDS_PER_MINUTE;
		seconds 	= seconds - ( minutes * SECONDS_PER_MINUTE );

		String result = "";

		boolean needComma = false;

		if ( days > 0 )
		{
			result = StringUtils.longToString( days ) +
				StringUtils.pluralize( (int)days, " day", " days" );

			needComma = true;
		}

		if ( ( hours + minutes + seconds ) > 0 )
		{
			if ( needComma ) result = result + ", ";

			needComma = false;

			if ( hours > 0 )
			{
				result = StringUtils.longToString( hours ) +
					StringUtils.pluralize( (int)hours, " hour", " hours" );

				needComma = true;
            }

			if ( ( minutes + seconds ) > 0 )
			{
				if ( minutes > 0 )
				{
					if ( needComma ) result = result + ", ";

					result = result + StringUtils.longToString( minutes ) +
						StringUtils.pluralize( (int)minutes, " minute", " minutes" );

					needComma = true;
                }

				if ( seconds > 0 )
				{
					if ( needComma )
						result = result + ", ";

					result = result + StringUtils.longToString( seconds ) +
						StringUtils.pluralize( (int)seconds, " second", " seconds" );
				}
			}
		}

		if ( result.length() == 0 )
			result = "0 seconds";

		return result;
	}

	/** Parses a timer value specified as "days hh:mm:ss".
	 *
	 *	@param	timerString		The timer string to be parsed.
	 *
	 *	@return					The timer value in milliseconds,
	 *							or -1 if the specified timer
	 *							value is bad.
	 */

	public static long parseTimer( String timerString )
	{
		long result = -1;
		long days	= 0;

		String[] tokens = StringUtils.makeTokenArray( timerString );

		switch ( tokens.length )
		{
			case 1:	try
					{
						days	= StringUtils.stringToLong( tokens[ 0 ] );
						result	= days * SECONDS_PER_DAY * 1000L;
					}
					catch ( Exception NumberFormatException )
					{
					}

					break;

			case 2: try
					{
						days = StringUtils.stringToLong( tokens[ 0 ] );
					}
					catch ( NumberFormatException e )
					{
						break;
					}

					StringTokenizer tokenizer =
						new StringTokenizer( tokens[ 1 ] , " :" );

					ArrayList hhmmss = new ArrayList();

					while ( tokenizer.hasMoreTokens() )
					{
						String token = (String)tokenizer.nextToken();
						hhmmss.add( token );
        			}

					if ( hhmmss.size() != 3 ) break;

					try
					{
						long hours =
							StringUtils.stringToLong( (String)hhmmss.get( 0 ) );

						if ( ( hours < 0 ) || ( hours > 23 ) ) break;

						long minutes =
							StringUtils.stringToLong( (String)hhmmss.get( 1 ) );

						if ( ( minutes < 0 ) || ( minutes > 59 ) ) break;

						long seconds =
							StringUtils.stringToLong( (String)hhmmss.get( 2 ) );

						if ( ( seconds < 0 ) || ( seconds > 59 ) ) break;

						result =
							makeTimerValue( days, hours, minutes, seconds );

					}
					catch ( NumberFormatException e )
					{
					}

					break;

			default:
					break;
		}

		return result;
	}

	/** Don't allow instantiation, do allow overrides. */

	protected DateTimeUtils()
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

