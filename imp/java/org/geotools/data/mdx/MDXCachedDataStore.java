package org.geotools.data.mdx;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import org.geotools.data.AbstractDataStore;
import org.geotools.data.DefaultServiceInfo;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.ServiceInfo;
import org.geotools.data.Transaction;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.FeatureTypes;
import org.olap4j.CellSet;
import org.olap4j.CellSetAxis;
import org.olap4j.OlapConnection;
import org.olap4j.OlapStatement;
import org.olap4j.OlapWrapper;
import org.olap4j.Position;
import org.olap4j.metadata.Member;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * DataStore that uses data caches for every MDX layer. This datastore will
 * perform an MDX query agains a QueryCatalog cube, containing the MDX queries
 * that must be 'layered'. (One layer per MDX query in that cube).
 *
 *
 * Based on the MDXDataStore implementation from Luca Morandini lmorandini@iee.org.
 *
 * @author fks/Serge de Schaetzen
 *
 */
public class MDXCachedDataStore extends AbstractDataStore
{
    // ===========================================================================
    /** Column name containing the layer name. */
    private static String NAMEATTRIBUTE = "Name";

    /** Column name containing the MDX query. */
    private static String QUERYATTRIBUTE = "MDX";

    /** Column name containing the geometry type. */
    private static String TYPEATTRIBUTE = "GeometryType";

    /** Column name containing the name of the geometry column. */
    private static String COLUMNATTRIBUTE = "GeometryColumn";

    /** Column name containing the type of the geometry column. */
    private static String COLUMNTYPE = "ColumnType";

    /** Defines the row axis of the olap result set. */
    public static int ROWAXIS = 1;

    /** Defines the column axis of the olap result set. */
    public static int COLUMNAXIS = 0;

    /** The olap server to connect to. */
    private String olapServer = null;

    /** The olap provider to use. */
    private String olapProvider = null;

    /** The olap datasource to use. */
    private String olapDataSource = null;

    /** The catalog to use. */
    private String olapCatalog = null;

    /** The user to use for the connection. */
    private String olapUser = null;

    /** The password to use for the connection. */
    private String olapPassword = null;

    /** Max amount of hours the data can remain cached. */
    private int refresh = 0;

    /** The srid to use. */
    private String srid = null;

    /** The map of catalog entries indexed on their type name. */
    private Map<String, MDXCatalogEntry> catalog = new HashMap<String, MDXCatalogEntry>();

    /** The active connection. */
    private OlapConnection connection = null;

    /** The processor to use, if any. */
    private MDXGeometryProcessor processor = null;

    /** The timestamp of the last refresh. */
    private long lastRefresh = 0;

    // ===========================================================================
    /**
     * Creates an instance.
     *
     * @param olapServer The server to connect to.
     * @param olapProvider The olap provider.
     * @param olapDataSource The olap datasource.
     * @param olapCatalog The olap catalog.
     * @param olapUser The olap userid to use.
     * @param olapPassword The password of that use.
     * @param srid The SRID to use.
     * @param refresh The amount of hours data can remain cached. (<= 0 is no cache).
     */
    public MDXCachedDataStore(String olapServer, String olapProvider, String olapDataSource, String olapCatalog, String olapUser, String olapPassword,
                              String srid, int refresh, MDXGeometryProcessor processor)
    {
        super(false); // does not allow writing

        this.olapCatalog = olapCatalog;
        this.olapDataSource = olapDataSource;
        this.olapPassword = olapPassword;
        this.olapProvider = olapProvider;
        this.olapServer = olapServer;
        this.olapUser = olapUser;
        this.refresh = refresh;
        this.srid = srid;
        this.processor = processor;

        loadCatalogEntries();
    }

    // ===========================================================================
    /**
     * Connects if not yet connected, and returns true if the connection succeeded.
     * (or if we were already connected.)
     */
    private boolean connect()
    {
        try
        {
            if (connection == null || connection.isClosed())
            {
                LOGGER.fine("About to connect with the MDX data source " + olapDataSource);
                Class.forName("org.olap4j.driver.xmla.XmlaOlap4jDriver");

                Connection jdbcConnection = DriverManager.getConnection("jdbc:xmla:Server=" + olapServer + ";Provider=" + olapProvider + ";DataSource=" + olapDataSource + ";Catalog=" + olapCatalog, olapUser, olapPassword);
                OlapWrapper wrapper = (OlapWrapper) jdbcConnection;
                connection = wrapper.unwrap(OlapConnection.class);
                LOGGER.fine("Connection with the MDX data source " + olapDataSource + " established");
            }
            return true;
        }
        catch (Exception e)
        {
            LOGGER.warning("Unable to open MDX data source " + e);
            return false;
        }
    }

    // ===========================================================================
    @Override
    public synchronized void dispose()
    {
        try
        {
            LOGGER.fine("MDX: Disposing MDX DataStore.");
            catalog.clear();
            if (connection != null)
            {
                connection.close();
                LOGGER.fine("Connection with the MDX data source " + olapDataSource + " closed");
            }
        }
        catch (Exception e)
        {
            LOGGER.warning("Unable to close MDX data source " + e);
        }
    }

    // ===========================================================================
    @Override
    public ServiceInfo getInfo()
    {
        DefaultServiceInfo info = new DefaultServiceInfo();
        info.setDescription("MDX Features from server " + this.olapServer);
        info.setSchema(FeatureTypes.DEFAULT_NAMESPACE);
        return info;
    }

    // ===========================================================================
    @Override
    public FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(Query query, Transaction transaction) throws IOException
    {
        loadCatalogEntries();
        MDXCatalogEntry entry = catalog.get(query.getTypeName());
        MDXDataCache cache = entry.getCache();
        return new MDXCachedFeatureReader(cache.getFeatures(query.getFilter()), cache.getFeatureType());
    }

    // ===========================================================================
    @Override
    public SimpleFeatureSource getFeatureSource(String typeName) throws IOException
    {
        loadCatalogEntries();
        MDXCatalogEntry entry = catalog.get(typeName);
        if (entry != null) return new MDXCachedFeatureSource(this, entry.getCache());
        else return null;
    }

    // ===========================================================================
    @Override
    public SimpleFeatureType getSchema(String typeName) throws IOException
    {
        loadCatalogEntries();
        MDXCatalogEntry entry = catalog.get(typeName);
        if (entry != null) return entry.getSchema();
        else return null;
    }

    // ===========================================================================
    @Override
    public String[] getTypeNames() throws IOException
    {
        loadCatalogEntries();
        return catalog.keySet().toArray(new String[catalog.size()]);
    }

    // ===========================================================================
    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(String typeName) throws IOException
    {
        loadCatalogEntries();
        MDXCatalogEntry entry = catalog.get(typeName);
        MDXDataCache cache = entry.getCache();
        return new MDXCachedFeatureReader(cache.getFeatures(), cache.getFeatureType());
    }

    // ===========================================================================
    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getFeatureReader(String typeName, Query query) throws IOException
    {
        loadCatalogEntries();
        MDXCatalogEntry entry = catalog.get(typeName);
        MDXDataCache cache = entry.getCache();
        return new MDXCachedFeatureReader(cache.getFeatures(query.getFilter()), cache.getFeatureType());
    }

    // ===========================================================================
    /**
     * Loads the catalog entries.
     */
    private synchronized void loadCatalogEntries()
    {
        if (catalog.isEmpty() || (System.currentTimeMillis() - lastRefresh > 30000))
        {
            connect();
            OlapStatement statement;

            try
            {
                statement = this.connection.createStatement();
                CellSet catResult = statement.executeOlapQuery("SELECT NON EMPTY {Hierarchize({[Measures].[MDX]})} ON COLUMNS, "
                        + "NON EMPTY CrossJoin([Name].[Name].Members, CrossJoin([MDX].[MDX].Members, "
                        + "CrossJoin([GeometryColumn].[GeometryColumn].Members, CrossJoin([ColumnType].[ColumnType].Members, "
                        + "[GeometryType].[GeometryType].Members)))) ON ROWS FROM [QueryCatalog]");

                CellSetAxis rowAxis = catResult.getAxes().get(ROWAXIS);
                for (Position rowPos : rowAxis.getPositions())
                {
                    MDXCatalogEntry entry = new MDXCatalogEntry();
                    for (Member member : rowPos.getMembers())
                    {
                        entry.addAttribute((String) (member.getDimension().getName()), member.getName());
                    }

                    MDXDataCache cache = new MDXDataCache(entry.getAttribute(NAMEATTRIBUTE), connection, entry.getAttribute(QUERYATTRIBUTE), srid,
                            entry.getAttribute(COLUMNATTRIBUTE), entry.getAttribute(TYPEATTRIBUTE), entry.getAttribute(COLUMNTYPE), refresh, processor);
                    entry.setCache(cache);
                    catalog.put(cache.getTypeName(), entry);
                }
                LOGGER.fine("Loaded OLAP query catalog");
                lastRefresh = System.currentTimeMillis();
            }
            catch (Throwable e)
            {
                LOGGER.warning("General Error reading the query catalog from MDX");
                e.printStackTrace();
            }
        }
    }
}
