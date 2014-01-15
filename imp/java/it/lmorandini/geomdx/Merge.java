package it.lmorandini.geomdx;

import java.util.List;

import mondrian.olap.Evaluator;
import mondrian.olap.Syntax;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.SetType;
import mondrian.olap.type.StringType;
import mondrian.olap.type.Type;
import mondrian.spi.UserDefinedFunction;

import org.apache.log4j.Logger;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.TopologyException;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/*
 * Returns the union of the geometries of a set
 */
public class Merge implements UserDefinedFunction
{

    private String name = null;

    public Merge(String name)
    {
        this.name = name;
    }

    public Type[] getParameterTypes()
    {
        return new Type[] {new SetType(MemberType.Unknown)};
    }

    public String[] getReservedWords()
    {
        return null;
    }

    public Type getReturnType(Type[] parameterTypes)
    {
        return new StringType();
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
        return "Merge(<Set>)";
    }


    @SuppressWarnings("unchecked")
    public Object execute(Evaluator evaluator, Argument[] arguments)
    {
        List<Object> geoms = (List<Object>) arguments[0].evaluate(evaluator);
        Geometry retval = null;

        Logger logger = Logger.getLogger(Contains.class);

        try
        {
            WKTReader wktReader = new WKTReader();

            for (Object geomWkt : geoms)
            {
                Geometry geom = wktReader.read((String) (geomWkt));
                if (retval == null)
                {
                    retval = geom;
                }
                else
                {
                    retval.union(geom);
                }
            }

        }
        catch (ParseException e)
        {
            logger.error("Error parsing WKT in user-defined function", e);
        }
        catch (TopologyException te)
        {
            logger.warn("TopologyException occured: " + te.toString() + "\n");
            if (logger.isDebugEnabled())
            {
                logger.debug("TopologyException stack trace", te);
            }
        }

        return retval;
    }

}
