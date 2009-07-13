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
    return os << "CoordinateVariable(" << var->get_name() << ")";
}

