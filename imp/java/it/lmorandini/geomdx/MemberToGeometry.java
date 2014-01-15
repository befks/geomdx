package it.lmorandini.geomdx;

import mondrian.olap.Evaluator;
import mondrian.olap.Member;
import mondrian.olap.Syntax;
import mondrian.olap.type.MemberType;
import mondrian.olap.type.StringType;
import mondrian.olap.type.Type;
import mondrian.spi.UserDefinedFunction;

import org.apache.log4j.Logger;

/*
 * Returns a geometry out of a Member. For the time being it just
 * use the name of the member, which is supposed to hold
 * a WKT string specifying a geometry.
 */
public class MemberToGeometry implements UserDefinedFunction
{

    private String name = null;

    public MemberToGeometry(String name)
    {
        this.name = name;
    }

    public Type[] getParameterTypes()
    {
        return new Type[] {MemberType.Unknown};
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
        return "MemberToGeometry(<Member>)";
    }

    public Object execute(Evaluator evaluator, Argument[] arguments)
    {
        String retval = null;

        Logger logger = Logger.getLogger(Contains.class);

        try
        {
            Member memberArg = (Member) (arguments[0].evaluate(evaluator));
            retval = (memberArg).getName();
        }
        catch (Exception e)
        {
            logger.error("Error converting member to String in user-defined function", e);
        }

        return retval;
    }

}
