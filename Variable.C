#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Attribute.H"
#include "Dimension.H"
#include "Variable.H"
#include "Util.H"


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

