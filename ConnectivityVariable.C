#include "ConnectivityVariable.H"


ConnectivityVariable::ConnectivityVariable(Variable *var, Dimension *to)
    :   VariableDecorator(var)
    ,   to(to)
{
}


ConnectivityVariable::~ConnectivityVariable()
{
    to = NULL;
}


Dimension* ConnectivityVariable::get_from_dim() const
{
    return var->get_dims()[0];
}


Dimension* ConnectivityVariable::get_to_dim() const
{
    return to;
}


ostream& ConnectivityVariable::print(ostream &os) const
{
    os << "ConnectivityVariable(" << var->get_name() << ")";
}

