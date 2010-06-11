#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Debug.H"
#include "Dimension.H"
#include "GlobalMask.H"
#include "Mask.H"
#include "Timing.H"


/**
 * Mask factory create method.
 *
 * Construct Mask based on the size of the given Dimension.
 *
 * @param dim Dimension to base size on
 */
Mask* Mask::create(Dimension *dim)
{
    Mask *mask;

    TIMING("Mask::create(Dimension*)");
    TRACER("Mask::create(Dimension*) size=%ld\n", dim->get_size());

    mask = new GlobalMask(dim);
    mask->fill();
    return mask;
}


/**
 * Constructor.
 */
Mask::Mask()
{
    TIMING("Mask::Mask()");
}


/**
 * Destructor.
 */
Mask::~Mask()
{
    TIMING("Mask::~Mask()");
}


ostream& operator << (ostream &os, const Mask *mask)
{
    return mask->print(os);
}
