package org.geotools.data.mdx;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.geotools.data.FeatureReader;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Feature reader that uses a cache to obtain the features.
 *
 * Based on the MDXFeatureReader implementation from Luca Morandini lmorandini@iee.org.
 *
 * @author fks/Serge de Schaetzen
 *
 */
public class MDXCachedFeatureReader implements FeatureReader<SimpleFeatureType, SimpleFeature>
{
    // ===========================================================================
    /** The iterator. */
    private Iterator<SimpleFeature> iterator = null;

    /** The feature type of the cached data. */
    private SimpleFeatureType featureType = null;

    // ===========================================================================
    /**
     * Creates an instance.
     *
     * @param features The list of cached features to handle.
     * @param type The type of data.
     */
    public MDXCachedFeatureReader(List<SimpleFeature> features, SimpleFeatureType type)
    {
        this.iterator = features.iterator();
        this.featureType = type;
    }

    // ===========================================================================
    @Override
    public void close() throws IOException
    {
        // Nothing to do as it uses cached data.
    }

    // ===========================================================================
    @Override
    public SimpleFeatureType getFeatureType()
    {
        return featureType;
    }

    // ===========================================================================
    @Override
    public boolean hasNext() throws IOException
    {
        return iterator.hasNext();
    }

    // ===========================================================================
    @Override
    public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException
    {
        return iterator.next();
    }
}
