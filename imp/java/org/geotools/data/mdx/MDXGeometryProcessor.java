package org.geotools.data.mdx;

import com.vividsolutions.jts.geom.Geometry;

/**
 * Defines a class that can be used to 'preprocess' MDX result data.
 *
 * @author fks/Serge de Schaetzen
 *
 */
public interface MDXGeometryProcessor
{
    // ===========================================================================
    /**
     * Requests the class to initialize itself using the given config
     * property.
     *
     * @param config The config to use.
     * @param srid The srid for the data to be processed.
     */
    void initialize(String config, int srid);

    // ===========================================================================
    /**
     * Requests the class to return a Geometry object from the given member.
     * If no geometry is found matching the criteria, null may be returned.
     *
     * @param member The member to process.
     * @param columnName The name of the current column.
     * @param An extra configuration parameter.
     * @return The Geometry value of that member.
     */
    Geometry getWKTString(String member, String columnName, String columnType);
}
