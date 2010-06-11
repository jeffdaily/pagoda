#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Dimension.H"
#include "Timing.H"


Dimension::Dimension()
{
    TIMING("Dimension::Dimension()");
}


Dimension::Dimension(const Dimension &copy)
{
    TIMING("Dimension::Dimension(Dimension)");
}


Dimension& Dimension::operator = (const Dimension &copy)
{
    TIMING("Dimension::operator=(Dimension)");
    if (&copy != this) {
    }
    return *this;
}


Dimension::~Dimension()
{
    TIMING("Dimension::~Dimension()");
}


ostream& operator << (ostream &os, const Dimension *other)
{
    TIMING("operator<<(ostream,Dimension*)");
    return other->print(os);
}
