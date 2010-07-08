#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Dataset.H"
#include "Dimension.H"
#include "Mask.H"
#include "MaskMap.H"
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


Mask* Dimension::get_mask() const
{
    Dataset *dataset = get_dataset();
    MaskMap *masks = dataset->get_masks();
    TIMING("Dimension::get_mask()");
    return masks->get_mask(this);
}


ostream& operator << (ostream &os, const Dimension *other)
{
    TIMING("operator<<(ostream,Dimension*)");
    return other->print(os);
}
