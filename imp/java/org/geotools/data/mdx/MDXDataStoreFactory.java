package org.geotools.data.mdx;

import java.io.IOException;
import java.io.Serializable;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;

/*
 * Factory to make new instances of MDX data stores
 *
 * @version $Id: MDXDataStoreFactory.java 414 2010-12-13 21:55:44Z lmorandini $
 * @author Luca Morandini lmorandini@iee.org
 */

public class MDXDataStoreFactory implements DataStoreFactorySpi
{

    public static final Param NAMESPACEURI = new Param("namespace", String.class, "Namespace of datastore", false);
    public static final Param SERVER = new Param("server", String.class, "URL of XMLA data source", true);
    public static final Param PROVIDER = new Param("provider", String.class, "Name of provider", true);
    public static final Param DATASOURCE = new Param("datasource", String.class, "Name of data source", true);
    public static final Param CATALOG = new Param("catalog", String.class, "Name of catalog", true);
    public static final Param USER = new Param("user", String.class, "Username", false);
    public static final Param PASSWORD = new Param("password", String.class, "Password", false);
    public static final Param REFRESH = new Param("refresh", String.class, "The max amount of hours the result of an Olap query is cached", true, "4");
    public static final Param SRID = new Param("srid", String.class, "The srid used by this store", true, "EPSG:4326");
    public static final Param PROCESSOR = new Param("GeometryProcessor", String.class, "The class name of the Geometry Processor", false);
    public static final Param PROCESSORCONFIG = new Param("GeometryProcessorConfig", String.class, "The config of the Geometry Processor", false);

    /*
     * Returns a new MDX data store from a Map of parameters
     *
     * @see org.geotools.data.DataStoreFactorySpi#createDataStore(java.util.Map)
     */
    public DataStore createDataStore(Map<String, Serializable> params) throws IOException
    {

        String server = (String) SERVER.lookUp(params);
        String provider = (String) PROVIDER.lookUp(params);
        String datasource = (String) DATASOURCE.lookUp(params);
        String catalog = (String) CATALOG.lookUp(params);
        String user = (String) USER.lookUp(params);
        String password = (String) PASSWORD.lookUp(params);
        String srid = (String) SRID.lookUp(params);
        String refresh = (String) REFRESH.lookUp(params);
        final String processorName = (String) PROCESSOR.lookUp(params);
        String config = (String) PROCESSORCONFIG.lookUp(params);

        if (this.canProcess(params))
        {
            try
            {
                int ref = Integer.parseInt(refresh);
                MDXGeometryProcessor processor = null;
                if (processorName != null)
                {
                    processor = AccessController.doPrivileged(new PrivilegedAction<MDXGeometryProcessor>()
                    {
                        @Override
                        public MDXGeometryProcessor run()
                        {
                            try
                            {
                                return (MDXGeometryProcessor) Class.forName(processorName).newInstance();
                            }
                            catch (Throwable e)
                            {
                                e.printStackTrace();
                                return null;
                            }
                        }
                    });
                    if (processor != null) processor.initialize(config, Integer.parseInt(srid.split(":")[1]));
                }

                MDXCachedDataStore store = new MDXCachedDataStore(server, provider, datasource, catalog, user, password, srid, ref, processor);
                return store;
            }
            catch (Throwable e)
            {
                e.printStackTrace();
                return null;
            }
        }
        else
        {
            throw new IOException("Missing parameter(s)");
        }
    }

    /*
     * MDX data stores cannot be created
     *
     * @see org.geotools.data.DataStoreFactorySpi#createNewDataStore(java.util.Map)
     */
    public DataStore createNewDataStore(Map<String, Serializable> params) throws IOException
    {
        System.out.println("MDX: Creating new MDXDataStore. Not allowed.");
        throw new IOException("An MDX data store can be istantiated, but not created");
    }

    /*
     * Checks the correctness of parameters
     *
     * @see org.geotools.data.DataAccessFactory#canProcess(java.util.Map)
     */
    public boolean canProcess(Map<String, Serializable> params)
    {

        if (params.get("server") != null && params.get("provider") != null && params.get("datasource") != null
         && params.get("catalog") != null && params.get("refresh") != null && params.get("srid") != null)
        {
            if (params.get("user") != null && params.get("password") == null) return false;
            else
            {
                if (params.get("GeometryProcessor") != null && params.get("GeometryProcessorConfig") == null) return false;
                else return true;
            }
        }

        return false;
    }

    /*
     * Returns data store description
     *
     * @see org.geotools.data.DataAccessFactory#getDescription()
     */
    public String getDescription()
    {
        return "Allows OLAP data sources to be queried by MDX";
    }

    /*
     * Returns the displayed data name
     *
     * @see org.geotools.data.DataAccessFactory#getDisplayName()
     */
    public String getDisplayName()
    {
        return "MDX data store";
    }

    /*
     * Returns a description of parameters needed to create the data store
     *
     * @see org.geotools.data.DataAccessFactory#getParametersInfo()
     */
    public Param[] getParametersInfo()
    {
        return new Param[] {NAMESPACEURI, SERVER, PROVIDER, DATASOURCE, CATALOG, USER, PASSWORD, REFRESH, SRID, PROCESSOR, PROCESSORCONFIG};
    }

    public boolean isAvailable()
    {
        return true;
    }

    /**
     * No implementation hints are provided at this time.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Map getImplementationHints()
    {
        return java.util.Collections.EMPTY_MAP;
    }

}
