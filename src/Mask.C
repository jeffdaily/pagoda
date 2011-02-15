#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include "DataType.H"
#include "Dimension.H"
#include "GlobalMask.H"
#include "Mask.H"


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
    :   AbstractArray(DataType::NOT_A_TYPE)
{
}


Mask::~Mask()
{
}


ostream& operator << (ostream &os, const Mask *mask)
{
    return mask->print(os);
}
