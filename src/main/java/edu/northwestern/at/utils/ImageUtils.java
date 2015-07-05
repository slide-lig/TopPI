package edu.northwestern.at.utils;

/*	Please see the license information at the end of this file. */

import java.awt.image.*;
import java.util.*;
import java.text.*;

/**	Image utilities.
 *
 *	<p>
 *	This static class provides various utility methods for manipulating
 *	images.
 *	</p>
 */

public class ImageUtils
{
	/**	Makes a ghost image suitable for dragging.
	 *
	 *	<p>All of the white pixels in the image are changed to transparent
	 *	pixels. All other pixels have their intensity halved.
	 *
	 *	@param	image		The buffered image.
	 */

	public static void makeGhost (BufferedImage image) {
		for (int i = 0; i < image.getWidth(); i++) {
			for (int j = 0; j < image.getHeight(); j++) {
				int pixel = image.getRGB(i, j);
				if (pixel == -1) {
					image.setRGB(i, j, 0);
				}
				else {
					int alpha = (pixel & 0xff000000);
					int red = (pixel & 0x00ff0000) >> 16;
					int green = (pixel & 0x0000ff00) >> 8;
					int blue = pixel & 0x000000ff;
					red = 255 - (255-red)/2;
					green = 255 - (255-green)/2;
					blue = 255 - (255-blue)/2;
					pixel = alpha | (red << 16) | (green << 8) | blue;
					image.setRGB(i, j, pixel);
				}
			}
		}
	}

	/** Don't allow instantiation, do allow overrides. */

	protected ImageUtils()
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

