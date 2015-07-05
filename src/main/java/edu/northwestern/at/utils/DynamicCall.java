package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.io.*;

/** DynamicCall.
 *
 *	<p>DynamicCall provides methods for dynamically calling methods in classes
 *	with specified arguments.  The class may be specified by its class type or
 *	by name.  The method is provided by name.  The list of parameters
 *	is passed as an {@link ArgumentList}.  DynamicCall uses Java reflection.</p>
 */

public class DynamicCall
{
	/** Use reflection to call a named method from a specified class.
	 *
	 *	@param	classToCall		The class containing the method to call.
	 *	@param	methodName		The method name to call.
	 *	@param	argumentList	The argument list.
	 */

	public static Object dynamicCall(
		Object classToCall,
		String methodName,
		ArgumentList argumentList )
		    throws 	NoSuchMethodException,
		    		InvocationTargetException,
	    			IllegalAccessException
	{
		Method methodToCall =
			classToCall.getClass().getMethod(
				methodName , argumentList.getArgumentClasses() );
		return ( methodToCall.invoke( classToCall , argumentList.getArguments() ) );
	}

	/** Use reflection to call a named method from a named class.
	 *
	 *	@param	className		The class name containing the method to call.
	 *	@param	methodName		The method name to call.
	 *	@param	argumentList	The argument list.
	 */

	public static Object dynamicCall(
		String className,
		String methodName,
		ArgumentList argumentList )
		    throws 	NoSuchMethodException,
		    		InvocationTargetException,
	    			IllegalAccessException,
	    			ClassNotFoundException,
					InstantiationException
	{
		Object result = null;

		try
		{
			java.lang.Class classToCall	= Class.forName( className );

			result	=
				dynamicCall(
					classToCall.newInstance() , methodName , argumentList );
		}
		catch ( ClassNotFoundException e )
		{
			throw new ClassNotFoundException();
		}

		return result;
	}

	/** Don't allow instantiation, do allow overrides. */

	protected DynamicCall()
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

