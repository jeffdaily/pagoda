#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Timing.H"
#include "Variable.H"


Variable::Variable()
{
    TIMING("Variable::Variable()");
}


Variable::~Variable()
{
    TIMING("Variable::~Variable()");
}


ostream& operator << (ostream &os, const Variable *other)
{
    TIMING("operator<<(ostream,Variable*)");
    return other->print(os);
}
