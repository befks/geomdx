package it.lmorandini.geomdx;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Locale;

import mondrian.olap.Evaluator;
import mondrian.olap.Member;
import mondrian.olap.Syntax;
import mondrian.olap.type.BooleanType;
import mondrian.olap.type.StringType;
import mondrian.olap.type.Type;
import mondrian.spi.UserDefinedFunction;

import org.apache.log4j.Logger;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.TopologyException;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/*
 * Returns true if the first argument (a member) contains the
 * second one (a string containing a WKT), false otherwise
 */
public class Contains implements UserDefinedFunction
{
    /** The array of all WKT Strings we can read. */
    private static final String[] WKT_TYPES = new String[] {"MULTIPOLYGON", "POLYGON", "LINESTRING", "MULTILINESTRING", "POINT", "MULTIPOINT", "LINEARRING"};

    private String name = null;

    public Contains(String name)
    {
        this.name = name;
    }

    public Type[] getParameterTypes()
    {
        return new Type[] {new StringType(), new StringType()};
    }

    public String[] getReservedWords()
    {
        return null;
    }

    public Type getReturnType(Type[] parameterTypes)
    {
        return new BooleanType();
    }

    public Syntax getSyntax()
    {
        return Syntax.Function;
    }

    public String getName()
    {
        return this.name;
    }

    public String getDescription()
    {
        return "Contains(<WKT String>, <WKT String>)";
    }

    /**
     * Parses the Geometry part in the given string.
     *
     * @param str The String to parse.
     * @return The Geometry.
     */
    private String parseGeometry(String str)
    {
        StringBuilder b = new StringBuilder();
        char[] chars = str.toCharArray();
        str = str.toUpperCase(Locale.getDefault());

        int start = -1;
        for (int i = 0; i < WKT_TYPES.length && start == -1; i++)
        {
            int index = str.indexOf(WKT_TYPES[i]);
            if (index > -1)
            {
                b.append(WKT_TYPES[i]);
                start = index + WKT_TYPES[i].length();
            }
        }

        if (start > -1)
        {
            int brackets = 0;
            for (int i = start; i < chars.length; i++)
            {
                char c = chars[i];
                switch (c)
                {
                    case '(' :
                        brackets++;
                        break;

                    case ')' :
                        brackets--;
                        if (brackets == 0)
                        {
                            b.append(c);
                            return b.toString();
                        }
                        break;

                    default:
                }

                b.append(c);
            }
        }

        return b.toString();
    }

    public Object execute(Evaluator evaluator, Argument[] arguments)
    {

        Object geomArg1 = arguments[0].evaluate(evaluator);
        Object geomArg2 = arguments[1].evaluate(evaluator);
        Boolean retval = Boolean.FALSE;

        Geometry geom1;
        Geometry geom2;

        Logger logger = Logger.getLogger(Contains.class);

        try
        {
            WKTReader wktReader = new WKTReader();
            String str1 = parseGeometry((String) (geomArg1));
            String str2 = parseGeometry((String) (geomArg2));
            System.out.println("Contains: 1 : " + str1.substring(0, 50) + "...");
            System.out.println("Contains: 2 : " + str2);
            geom1 = wktReader.read(str1);
            geom2 = wktReader.read(str2);

            retval = new Boolean(geom1.overlaps(geom2));

        }
        catch (ParseException e)
        {
            logger.error("Error parsing WKT in user-defined function", e);
        }
        catch (TopologyException te)
        {
            Member[] members = evaluator.getMembers();
            logger.warn("TopologyException occured: " + te.toString() + "\n" + "  current context members: " + members.toString());
            if (logger.isDebugEnabled())
            {
                logger.debug("TopologyException stack trace", te);
            }
        }

        return retval;
    }


    public static void main(String[] args)
    {
        try
        {
            BufferedReader in = new BufferedReader(new FileReader("c:/temp/geom"));
            StringBuilder b = new StringBuilder();
            while (in.ready()) b.append(in.readLine());
            in.close();
            WKTReader wktReader = new WKTReader();
            Geometry geom1 = wktReader.read(b.toString());
            Geometry geom2 = wktReader.read("Polygon((2.57531 50.985943, 2.57531 51.098177, 2.7861309999999997 51.098177, 2.57531 50.985943))");
            System.out.println(geom1.getGeometryType());
            System.out.println("Overlaps: " + geom1.overlaps(geom2));
            System.out.println("Covers  : " +geom1.covers(geom2));
            System.out.println("Contains: " +geom1.contains(geom2));
            System.out.println("BB : " + geom1.getEnvelopeInternal().toString());
        }
        catch (Throwable e)
        {
            e.printStackTrace();
        }
    }
}
