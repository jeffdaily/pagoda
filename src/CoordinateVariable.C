#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "CoordinateVariable.H"
#include "Timing.H"


CoordinateVariable::CoordinateVariable(Variable *var)
    :   VariableDecorator(var)
{
    TIMING("CoordinateVariable::CoordinateVariable(Variable*)");
}


CoordinateVariable::~CoordinateVariable()
{
    TIMING("CoordinateVariable::~CoordinateVariable()");
}


ostream& CoordinateVariable::print(ostream &os) const
{
    TIMING("CoordinateVariable::print(ostream)");
    const string name = var->get_name();
    return os << "CoordinateVariable(" << name << ")";
}

