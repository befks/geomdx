package org.geotools.data.mdx;

import java.util.HashMap;

import org.opengis.feature.simple.SimpleFeatureType;

/*
 * $Id: MDXCatalogEntry.java 422 2010-12-26 16:16:36Z lmorandini $
 *
 * Single entry of the OLAP catalog
 */
public class MDXCatalogEntry
{
    /** The cache for that entry. */
    private MDXDataCache cache = null;
    private HashMap<String, String> map = new HashMap<String, String>();
    private SimpleFeatureType schema;

    // ===========================================================================
    /**
     * Adds an attibute.
     *
     * @param key The attribute name.
     * @param value The value.
     */
    public void addAttribute(String key, String value)
    {
        map.put(key, value);
    }

    // ===========================================================================
    /**
     * Returns the value for the attribute with the given name, or null if no
     * such attribute is present.
     *
     * @param key The name of the attribute.
     * @return The (possibly null) value of the attribute.
     */
    public String getAttribute(String key)
    {
        return (String) map.get(key);
    }

    // ===========================================================================
    /**
     * Sets feature type.
     *
     * @param schema The feature type.
     */
    public void setSchema(SimpleFeatureType schema)
    {
        this.schema = schema;
    }

    // ===========================================================================
    /**
     * Returns the feature type.
     *
     * @return The feature type.
     */
    public SimpleFeatureType getSchema()
    {
        if (cache != null) return cache.getFeatureType();
        else return this.schema;
    }

    // ===========================================================================
    /**
     * @return the cache.
     */
    public MDXDataCache getCache()
    {
        return cache;
    }

    // ===========================================================================
    /**
     * @param cache the cache to set.
     */
    public void setCache(MDXDataCache cache)
    {
        this.cache = cache;
    }
}
