package org.geotools.data.mdx;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.geotools.geometry.jts.WKTReader2;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

/**
 * Feathers specific WKTCache that caches the WKT information, loaded from a file.
 *
 * @author fks/Serge de Schaetzen
 *
 */
public class FeathersWKTCache implements MDXGeometryProcessor
{
    // ===========================================================================
    /** The map of zone geometries indexed on the zone id. */
    private Map<String, Geometry> zones = new HashMap<String, Geometry>();

    /** The map of subzone geometries indexed on the subzone id. */
    private Map<String, Geometry> subzones = new HashMap<String, Geometry>();

    /** The map of superzone geometries indexed on the superzone id. */
    private Map<String, Geometry> superzones = new HashMap<String, Geometry>();

    // ===========================================================================
    @Override
    public void initialize(String config, int srid)
    {
        try
        {
            PrecisionModel model = new PrecisionModel(PrecisionModel.FLOATING);
            WKTReader2 geometryReader = new WKTReader2(new GeometryFactory(model, srid));

            BufferedReader reader = new BufferedReader(new InputStreamReader(new URL(config).openStream()));

            while (reader.ready())
            {
                String line = reader.readLine();
                String[] fields = line.split("#");
                if (!subzones.containsKey(fields[1])) subzones.put(fields[1], geometryReader.read(fields[2]));
                if (!zones.containsKey(fields[3])) zones.put(fields[3], geometryReader.read(fields[4]));
                if (!superzones.containsKey(fields[5])) superzones.put(fields[5], geometryReader.read(fields[6]));
            }

            reader.close();
        }
        catch (Throwable e)
        {
            e.printStackTrace();
        }
    }

    // ===========================================================================
    @Override
    public Geometry getWKTString(String member, String columnName, String type)
    {
        if ("zone".equals(type)) return zones.get(member);
        else if ("subzone".equals(type)) return subzones.get(member);
        else if ("superzone".equals(type)) return superzones.get(member);
        else return null;
    }

    public static void main(String[] args)
    {
        String url = new String("file:///c:/tmp/geography.txt");
        FeathersWKTCache c = new FeathersWKTCache();
        c.initialize(url, 4326);
        System.out.println("        Zone 1 : " + (c.getWKTString("1", "", "zone") != null));
        System.out.println("SuperZone 4554 : " + (c.getWKTString("4554", "", "superzone") != null));
        System.out.println("   SubZone 123 : " + (c.getWKTString("123", "", "subzone") != null));
    }
}
