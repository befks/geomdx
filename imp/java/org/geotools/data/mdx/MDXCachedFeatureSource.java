package org.geotools.data.mdx;

import java.io.IOException;
import java.util.List;

import org.geotools.data.AbstractFeatureSource;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureListener;
import org.geotools.data.Query;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

/**
 * A FeatureSource that obtains its data from an MDXDataCache.
 *
 * Based on the MDXFeatureSource implementation from Luca Morandini lmorandini@iee.org.
 *
 * @author fks/Serge de Schaetzen
 *
 */
@SuppressWarnings("unchecked")
public class MDXCachedFeatureSource extends AbstractFeatureSource
{
    // ===========================================================================
    /** The datastore to use. */
    private DataStore store = null;

    /** The cache to use. */
    private MDXDataCache cache = null;

    // ===========================================================================
    /**
     * Creates an instance.
     *
     * @param store The store to use.
     * @param cache The cache to use.
     */
    public MDXCachedFeatureSource(DataStore store, MDXDataCache cache)
    {
        this.store = store;
        this.cache = cache;
    }

    // ===========================================================================
    @Override
    public ReferencedEnvelope getBounds() throws IOException
    {
        return getEnvelope(cache.getFeatures());
    }

    // ===========================================================================
    @Override
    public ReferencedEnvelope getBounds(Query query) throws IOException
    {
        return getEnvelope(cache.getFeatures(query.getFilter()));
    }

    // ===========================================================================
    /**
     * Creates and returns a reference envelope large enough to contain all
     * given features.
     *
     * @param features The features to process.
     * @return The envelope.
     */
    private ReferencedEnvelope getEnvelope(List<SimpleFeature> features)
    {
        ReferencedEnvelope ext = new ReferencedEnvelope();

        for (SimpleFeature feature : features)
            ext.expandToInclude(new ReferencedEnvelope(feature.getBounds()));

        return ext;
    }

    // ===========================================================================
    @Override
    public DataStore getDataStore()
    {
        return store;
    }

    // ===========================================================================
    @Override
    public SimpleFeatureCollection getFeatures() throws IOException
    {
        ListFeatureCollection collection = new ListFeatureCollection(cache.getFeatureType());
        collection.addAll(cache.getFeatures());
        return collection;
    }

    // ===========================================================================
    @Override
    public SimpleFeatureCollection getFeatures(Filter filter) throws IOException
    {
        ListFeatureCollection collection = new ListFeatureCollection(cache.getFeatureType());
        collection.addAll(cache.getFeatures(filter));
        return collection;
    }

    // ===========================================================================
    @Override
    public SimpleFeatureCollection getFeatures(Query query) throws IOException
    {
        ListFeatureCollection collection = new ListFeatureCollection(cache.getFeatureType());
        collection.addAll(cache.getFeatures(query.getFilter()));
        return collection;
    }

    // ===========================================================================
    @Override
    public SimpleFeatureType getSchema()
    {
        return cache.getFeatureType();
    }

    // ===========================================================================
    @Override
    public void addFeatureListener(FeatureListener l)
    {
    }

    // ===========================================================================
    @Override
    public void removeFeatureListener(FeatureListener arg0)
    {
    }
}
