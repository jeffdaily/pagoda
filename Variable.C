#include "Variable.H"


Variable::Variable()
{
}


Variable::~Variable()
{
}


ostream& operator << (ostream &os, const Variable *other)
{
    return other->print(os);
}

