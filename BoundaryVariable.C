#include "BoundaryVariable.H"


BoundaryVariable::BoundaryVariable(Variable *bound, Variable *bounded_var)
    :   VariableDecorator(bound)
    ,   bounded_var(bounded_var)
{
}


BoundaryVariable::~BoundaryVariable()
{
    bounded_var = NULL;
}


Variable* BoundaryVariable::get_bounded_var() const
{
    return bounded_var;
}


ostream& BoundaryVariable::print(ostream &os) const
{
    return os << "BoundaryVariable(" << var->get_name() << ")";
}
