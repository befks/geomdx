package org.geotools.data.mdx;

import static org.geotools.data.mdx.MDXCachedDataStore.COLUMNAXIS;
import static org.geotools.data.mdx.MDXCachedDataStore.ROWAXIS;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.geotools.data.DataUtilities;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.WKTReader2;
import org.olap4j.Cell;
import org.olap4j.CellSet;
import org.olap4j.OlapConnection;
import org.olap4j.OlapStatement;
import org.olap4j.Position;
import org.olap4j.metadata.Hierarchy;
import org.olap4j.metadata.Member;
import org.olap4j.metadata.NamedList;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * Defines an MDX query resultset cache. The cache is lazy initialized, meaning that
 * the data is fetched the first time a request is made for it.
 *
 * @author fks/Serge de Schaetzen
 *
 */
public class MDXDataCache
{
    // ===========================================================================
    /** The logger instance to use. */
    private static Logger logger = org.geotools.util.logging.Logging.getLogger("org.geotools.data.mdx");

    /** The type name of the features. */
    private String typeName = null;

    /** The connection to use. */
    private OlapConnection connection = null;

    /** The MDX Query to execute. */
    private String mdxQuery = null;

    /** The SRID. */
    private int srid = 0;

    /** The name of the column to be used to obtain the Geometry. */
    private String wktColumn = null;

    /** The Geometry type of that column. */
    private String geometryType = null;

    /** The extra type of the WKT column. */
    private String columnType = null;

    /** The maximum amount of hours the data is kept in the cache. */
    private long maxHours = 0;

    /** Timestamp of the last refresh. */
    private long lastRefresh = 0;

    /** The feature type of the data. */
    private SimpleFeatureType featureType = null;

    /** The list of available features. */
    private List<SimpleFeature> features = new ArrayList<SimpleFeature>();

    /** The processor to use, if any. */
    private MDXGeometryProcessor processor = null;

    // ===========================================================================
    /**
     * Creates an instance.
     *
     * @param typeName The type name.
     * @param connection The active connection.
     * @param mdxQuery The query to execute.
     * @param srid The SRID.
     * @param wktColumn The column to obtain the Geometry.
     * @param columnType The type of that column.
     * @param geometryType The geometry type of that column.
     * @param maxHours The max amount of hours the resultset is kept in memory.
     * @param processor The processor to use. (if any).
     */
    public MDXDataCache(String typeName, OlapConnection connection, String mdxQuery, String srid, String wktColumn, String geometryType, String columnType, int maxHours, MDXGeometryProcessor processor)
    {
        this.typeName = typeName;
        this.connection = connection;
        this.mdxQuery = mdxQuery;
        this.columnType = columnType;
        this.srid = Integer.parseInt(srid.split(":")[1]);
        this.wktColumn = wktColumn;
        this.geometryType = geometryType;
        this.maxHours = maxHours;
        this.processor = processor;
    }

    // ===========================================================================
    /**
     * Returns the typename.
     *
     * @return The type name.
     */
    public String getTypeName()
    {
        return typeName;
    }

    // ===========================================================================
    /**
     * Returns the list of available features. The list returned is a copy of the
     * internal list, and hence may be altered without affecting this class.
     *
     * @return The list of (unfiltered) features.
     */
    public List<SimpleFeature> getFeatures()
    {
        return getFeatures(null);
    }

    // ===========================================================================
    /**
     * Returns the list of available features. The list returned is a copy of the
     * internal list, and hence may be altered without affecting this class.
     *
     * The list will only contain the features that passes the given filter.
     *
     * @return The list of filtered features.
     */
    public List<SimpleFeature> getFeatures(Filter filter)
    {
        synchronized (features)
        {
            checkAndFillCache();
            List<SimpleFeature> l = null;
            if (filter != null)
            {
                l = new ArrayList<SimpleFeature>();
                for (SimpleFeature feature : features)
                    if (filter.evaluate(feature)) l.add(feature);
            }
            else
            {
                l = new ArrayList<SimpleFeature>(features.size());
                l.addAll(features);
            }

            return l;
        }
    }

    // ===========================================================================
    /**
     * Returns the feature type for this cache.
     *
     * @return The feature type.
     */
    public SimpleFeatureType getFeatureType()
    {
        synchronized (features)
        {
            checkAndFillCache();
            return featureType;
        }
    }

    // ===========================================================================
    /**
     * Checks the cache and fills (or refills) it if necessary. That is when the
     * last time the cache was filled is more than maxHours ago.
     */
    private void checkAndFillCache()
    {
        synchronized (features)
        {
        if (System.currentTimeMillis() - lastRefresh > maxHours)
            {
                try
                {
                    logger.log(Level.FINE, "Refreshing cached data.");

                    OlapStatement stmt = connection.createStatement();
                    CellSet result = stmt.executeOlapQuery(mdxQuery);

                    StringBuffer attributes = new StringBuffer("");
                    List<Hierarchy> hiers = result.getAxes().get(ROWAXIS).getAxisMetaData().getHierarchies();

                    // This feature will contain the geometry object.
                    for (Hierarchy hier : hiers)
                    {
                        if (processor != null || !hier.getName().equals(wktColumn))
                            attributes.append(hier.getName() + ":String,");
                    }

                    String check = mdxQuery.toLowerCase();

                    NamedList<Member> members = result.getAxes().get(COLUMNAXIS).getAxisMetaData().getHierarchies().get(0).getRootMembers();
                    for (Member memb : members)
                    {
                        if (!memb.getMemberType().name().equals("ALL"))
                        {
                            String membName = "[" + memb.getHierarchy().getName() + "].[" + memb.getName() + "]";
                            if (check.indexOf(membName.toLowerCase()) > -1)
                                attributes.append(memb.getName() + ":Float,");
                        }
                        else
                        {
                            for (Member mm : memb.getChildMembers())
                            {
                                String level = "[" + mm.getLevel().getHierarchy().getName() + "].[" + mm.getLevel().getName() + "]";
                                String membName = "[" + mm.getLevel().getName() + "].[" + mm.getName() + "]";
                                if (check.indexOf(level.toLowerCase()) > -1 || check.indexOf(membName.toLowerCase()) > -1)
                                    attributes.append(mm.getName() + ":Float,");
                            }
                        }
                    }
                    attributes.deleteCharAt(attributes.length() - 1);
                    if (processor != null) featureType = DataUtilities.createType(typeName, "MDXGeometry:" + geometryType + ":srid=" + srid + "," + attributes);
                    else featureType = DataUtilities.createType(typeName, wktColumn + ":" + geometryType + ":srid=" + srid + "," + attributes);

                    processCellSet(result);
                    lastRefresh = System.currentTimeMillis();
                }
                catch (Throwable e)
                {
                    logger.log(Level.SEVERE, "An error occurred filling the cache.", e);
                }
            }
        }
    }

    // ===========================================================================
    /**
     * Processes the given result set and fills the cache.
     *
     * @param result The resultset to process.
     * @throws Throwable If an error occurs.
     */
    private void processCellSet(CellSet result) throws Throwable
    {
        features.clear();
        PrecisionModel model = new PrecisionModel(PrecisionModel.FLOATING);
        WKTReader2 reader = new WKTReader2(new GeometryFactory(model, srid));

        ListIterator<Position> rowIter = result.getAxes().get(ROWAXIS).iterator();
        while (rowIter.hasNext())
        {
            Position rowPos = rowIter.next();
            if (!rowPos.getMembers().get(0).isAll())
            {
                SimpleFeatureBuilder builder = new SimpleFeatureBuilder(this.featureType);
                SimpleFeature feature = builder.buildFeature(null);

                for (Member member : rowPos.getMembers())
                {
                    String dimName = member.getUniqueName().split("\\[|\\]")[1];
                    if (dimName.equals(this.wktColumn))
                    {
                        Geometry geom = null;
                        if (processor != null)
                        {
                            geom = processor.getWKTString(member.getName(), wktColumn, columnType);
                            feature.setAttribute(this.wktColumn, member.getName());
                            feature.setAttribute("MDXGeometry", geom);
                        }
                        else
                        {
                            geom = reader.read(member.getName());
                            feature.setAttribute(this.wktColumn, geom);
                        }
                    }
                    else
                    {
                        String value = (member == null) ? "NULL" : (member.getName());
                        feature.setAttribute(dimName, value);
                    }
                }

                for (Position colPos : result.getAxes().get(COLUMNAXIS).getPositions())
                {
                    Cell cell = result.getCell(colPos, rowPos);
                    String value = (cell.getValue() == null) ? "0" : (cell.getValue().toString());
                    feature.setAttribute(colPos.getMembers().get(0).getName(), Float.parseFloat(value));
                }

                features.add(feature);
            }
        }
    }
}
