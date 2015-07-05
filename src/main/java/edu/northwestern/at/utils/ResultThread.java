package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.lang.*;

/**	Thread class that provides an object for storing results.
 *
 *	<p>
 *	All of the constructors are copied from Thread.
 *	</p>
 */

public class ResultThread extends Thread
{
	/**	The object in which results may be stored. */

	public Object result = null;

	/**	Replicate all the standard Thread constructors. */

	public ResultThread()
	{
		super();
	}

	public ResultThread( Runnable target )
	{
		super( target );
	}

	public ResultThread( Runnable target , String name )
	{
		super( target , name );
	}

	public ResultThread( String name )
	{
		super( name );
	}

	public ResultThread( ThreadGroup group , Runnable target )
	{
		super( group , target );
	}

	public ResultThread( ThreadGroup group , Runnable target , String name )
	{
		super( group , target , name );
	}

	public ResultThread(	ThreadGroup group , Runnable target , String name ,
							long stackSize )
	{
		super( group , target , name , stackSize );
	}

	public ResultThread( ThreadGroup group , String name )
	{
		super( group , name );
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

