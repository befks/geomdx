package it.lmorandini.geomdx;

import mondrian.olap.Evaluator;
import mondrian.olap.Member;
import mondrian.olap.Syntax;
import mondrian.olap.type.BooleanType;
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
 * Returns true if the first argument (a member) is distant
 * from the secondo argument (a geometry expressed as a WKT string)
 * less than the third argument (a numeric), false otherwise
 */
public class IsWithinDistance implements UserDefinedFunction
{

    private String name = null;

    public IsWithinDistance(String name)
    {
        this.name = name;
    }

    public Type[] getParameterTypes()
    {
        return new Type[] {new StringType(), new StringType(), new NumericType()};
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
        return "IsWithinDistance(<WKTString>, <WKTString>, <Numeric>)";
    }

    public Object execute(Evaluator evaluator, Argument[] arguments)
    {

        Object geomArg1 = arguments[0].evaluate(evaluator);
        Object geomArg2 = arguments[1].evaluate(evaluator);
        Object distanceArg = arguments[2].evaluate(evaluator);
        Boolean retval = Boolean.FALSE;

        Geometry geom1;
        Geometry geom2;

        Logger logger = Logger.getLogger(Contains.class);

        try
        {
            WKTReader wktReader = new WKTReader();
            geom1 = wktReader.read((String) (geomArg1));
            geom2 = wktReader.read((String) (geomArg2));
            double distance = ((Double) (distanceArg)).doubleValue();

            retval = new Boolean(geom1.isWithinDistance(geom2, distance));

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
