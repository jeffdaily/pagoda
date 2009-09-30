#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Dimension.H"
#include "Timing.H"


Dimension::Dimension()
    :   mask(NULL)
{
    TIMING("Dimension::Dimension()");
}


Dimension::Dimension(const Dimension &copy)
    :   mask(copy.mask)
{
    TIMING("Dimension::Dimension(Dimension)");
}


Dimension& Dimension::operator = (const Dimension &copy)
{
    TIMING("Dimension::operator=(Dimension)");
    if (&copy != this) {
        mask = copy.mask;
    }
    return *this;
}


Dimension::~Dimension()
{
    TIMING("Dimension::~Dimension()");
}


void Dimension::set_mask(Mask *mask)
{
    TIMING("Dimension::set_mask(Mask*)");
    this->mask = mask;
}


Mask* Dimension::get_mask() const
{
    TIMING("Dimension::get_mask()");
    return mask;
}


ostream& operator << (ostream &os, const Dimension *other)
{
    TIMING("operator<<(ostream,Dimension*)");
    return other->print(os);
}
