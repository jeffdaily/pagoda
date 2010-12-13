#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Debug.H"
#include "Dimension.H"
#include "GlobalMask.H"
#include "Mask.H"
#include "Timing.H"


Mask* Mask::create(const string &name, int64_t size)
{
    Mask *mask;

    mask = new GlobalMask(name, size);
    mask->reset();

    return mask;
}


Mask* Mask::create(const Dimension *dim)
{
    return Mask::create(dim->get_name(), dim->get_size());
}


Mask::Mask()
    :   AbstractArray()
{
    TIMING("Mask::Mask()");
}


Mask::~Mask()
{
    TIMING("Mask::~Mask()");
}


ostream& operator << (ostream &os, const Mask *mask)
{
    return mask->print(os);
}
