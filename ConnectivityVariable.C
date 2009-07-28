#include "ConnectivityVariable.H"
#include "Dimension.H"
#include "DistributedMask.H"
#include <ga.h>


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


void ConnectivityVariable::reindex()
{
    std::cout << "ConnectivityVariable::reindex BEGIN" << std::endl;
    if (to->get_mask() && to->get_mask()->was_cleared()) {
        std::cout << "reindexing" << std::endl;
        to->get_mask()->reindex();
        DistributedMask *mask = dynamic_cast<DistributedMask*>(to->get_mask());
        if (mask) {
            //std::cout << "printint mask" << std::endl;
            //GA_Print(mask->get_handle_index());
        }
    }
    std::cout << "ConnectivityVariable::reindex END" << std::endl;
}


ostream& ConnectivityVariable::print(ostream &os) const
{
    return os << "ConnectivityVariable(" << var->get_name() << ")";
}

