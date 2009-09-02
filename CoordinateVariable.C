#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "CoordinateVariable.H"


CoordinateVariable::CoordinateVariable(Variable *var)
    :   VariableDecorator(var)
{
}


CoordinateVariable::~CoordinateVariable()
{
}


ostream& CoordinateVariable::print(ostream &os) const
{
    const string name = var->get_name();
    return os << "CoordinateVariable(" << name << ")";
}

