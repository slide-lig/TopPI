package fr.liglab.hyptest.math;

/*	Please see the license information at the end of this file. */

import edu.northwestern.at.utils.math.Factorial;

/** Calculate Fisher's exact test for a 2x2 frequency table.
 */

public class FishersExactTest
{
	/**
	 * p-values will be adjusted such that ADJUSTMENT_RANGE*a+ln(P(unionSupport)) fits in [-ADJUSTMENT_RANGE/2;  ADJUSTMENT_RANGE/2]
	 */
	public static final int ADJUSTMENT_RANGE = 1000;
	
	/**	
	 *	@param	a		Frequency for cell(1,1).
	 *	@param	b		Frequency for cell(1,2).
	 *	@param	c		Frequency for cell(2,1).
	 *	@param	d		Frequency for cell(2,2).
	 *	@return	correction factor (ie. how many times should we add/substract ADJUSTMENT_RANGE to have a log(factorials) in [-ADJUSTMENT_RANGE/2, ADJUSTMENT_RANGE/2]
	 */
	public static long correctionFactor
	(
		int a ,
		int b ,
		int c ,
		int d
	)
	{
		//	Compute parameters for hypergeometric
		//	distribution.
		int r	= a + b;
		int s	= c + d;
		int m	= a + c;
		int n	= b + d;
				/*	Get range of variation. */
		
		int lm	= ( 0 > m - s ) ? 0 : m - s;
		int um	= ( m < r   ) ? m : r;
		
				/*	Probability is 1 if no range of variation. */
		
		if ( ( um - lm + 2 ) == 0 )
		{
			return 0;
		}
		
		int ndd	= m+n;
		
		if ( a < ((long) r)*((long) m)/ndd ) {
			return 0;
		} else {
			double sum	=
				logCombination( r ,  a ) +
				logCombination( s , c ) -
				logCombination( ndd , m );
			
			return Math.round(-sum/ADJUSTMENT_RANGE);
		}
	}
	
	
	/**	Calculate Fisher's exact test from the four cell counts.
	 *
	 *	@param	a		Frequency for cell(1,1).
	 *	@param	b		Frequency for cell(1,2).
	 *	@param	c		Frequency for cell(2,1).
	 *	@param	d		Frequency for cell(2,2).
	 *  @param  factor	correction factor (ie. how many times should we add/substract ADJUSTMENT_RANGE to have a log(factorials) in [-ADJUSTMENT_RANGE/2, ADJUSTMENT_RANGE/2]
	 *
	 *	@return			right-tail Fisher's exact test.
	 */

	public static double fishersExactTest
	(
		int a ,
		int b ,
		int c ,
		int d ,
		long factor
	)
	{
		double result;

								//	Compute parameters for hypergeometric
								//	distribution.
		int r	= a + b;
		int s	= c + d;
		int m	= a + c;
		int n	= b + d;
								/*	Get range of variation. */

		int lm	= ( 0 > m - s ) ? 0 : m - s;
		int um	= ( m < r   ) ? m : r;

								/*	Probability is 1 if no range of variation. */

		if ( ( um - lm + 2 ) == 0 )
		{
			result = 1.0D;
		}
		else
		{
			result = 0.0D;
			
			for ( int i = a ; i <= um ; i++ )
			{
				double probability	=
					hypergeometricProbability( i , r , s , m , n , factor);

				result += probability;

			}
		}

		return result;
	}

	/**	Compute log of number of combinations of n things taken k at a time.
	 *
	 *	@param	n	Number of items.
	 *	@param	k	Group size.
	 *
	 *	@return		Value for log of n items taken k at a time.
	 *
	 *	<p>
	 *	log(combination(n,m))	= log(n!/m!(n-m)!)
	 *							= log(n!) - log(m!) - log((n-m)!)
	 *	</p>
	 */

	protected static double logCombination( int n , int k )
	{
		return
			Factorial.logFactorial( n ) -
			Factorial.logFactorial( k ) -
			Factorial.logFactorial( n - k );
	}

	/**	Compute hypergeometric probability.
	 *
	 *	@param	x
	 *	@param	n1d
	 *	@param	n2d
	 *	@param	nd1
	 *	@param	nd2
	 *  @param factor multiplied by ADJUSTMENT_RANGE to keep the intermediate logCombination in ]-ADJUSTMENT_RANGE/2;ADJUSTMENT_RANGE/2]
	 *
	 *	@return		The hypergeometric probability.
	 */

	protected static double hypergeometricProbability
	(
		int x,
		int n1d,
		int n2d,
		int nd1,
		int nd2,
		long factor
	)
	{
		int n3	= nd1 - x;
		int ndd	= nd1 + nd2;

		double sum	=
			logCombination( n1d , x ) +
			logCombination( n2d , n3 ) -
			logCombination( ndd , nd1 );
		return Math.exp( factor*ADJUSTMENT_RANGE + sum );
	}
	

	/**	Don't allow instantiation but do allow overrides.
	 */

	protected FishersExactTest()
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

