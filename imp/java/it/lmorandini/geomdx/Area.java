package it.lmorandini.geomdx;

import mondrian.olap.Evaluator;
import mondrian.olap.Member;
import mondrian.olap.Syntax;
import mondrian.olap.type.NumericType;
import mondrian.olap.type.StringType;
import mondrian.olap.type.Type;
import mondrian.spi.UserDefinedFunction;

import org.apache.log4j.Logger;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.TopologyException;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/*
 * Returns the computed area of a geometry
 */
public class Area implements UserDefinedFunction
{

    private String name = null;

    public Area(String name)
    {
        this.name = name;
    }

    public Type[] getParameterTypes()
    {
        return new Type[] {new StringType()};
    }

    public String[] getReservedWords()
    {
        return null;
    }

    public Type getReturnType(Type[] parameterTypes)
    {
        return new NumericType();
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
        return "Area(<WKT String>)";
    }

    public Object execute(Evaluator evaluator, Argument[] arguments)
    {

        Object geomArg1 = arguments[0].evaluate(evaluator);
        Geometry geom;
        Double retval = new Double(0);

        Logger logger = Logger.getLogger(Contains.class);

        try
        {
            WKTReader wktReader = new WKTReader();
            geom = wktReader.read((String) (geomArg1));

            retval = new Double(geom.getArea());

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

}
