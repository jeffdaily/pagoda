#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

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

