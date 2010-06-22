#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "BoundaryVariable.H"
#include "Timing.H"


BoundaryVariable::BoundaryVariable(Variable *bound, Variable *bounded_var)
    :   VariableDecorator(bound)
    ,   bounded_var(bounded_var)
{
    TIMING("BoundaryVariable::BoundaryVariable(...)");
}


BoundaryVariable::~BoundaryVariable()
{
    TIMING("BoundaryVariable::~BoundaryVariable()");
    bounded_var = NULL;
}


Variable* BoundaryVariable::get_bounded_var() const
{
    TIMING("BoundaryVariable::get_bounded_var()");
    return bounded_var;
}


ostream& BoundaryVariable::print(ostream &os) const
{
    TIMING("BoundaryVariable::print(ostream)");
    const string name = var->get_name();
    return os << "BoundaryVariable(" << name << ")";
}
