#include "Dimension.H"
#include "Mask.H"

Mask::Mask(Dimension *dim)
    :   dim(dim)
    ,   dirty(true)
    ,   cleared(false)
    ,   count(0)
{
}


Mask::~Mask()
{
}


string Mask::get_name() const
{
    return dim->get_name();
}


Dimension* Mask::get_dim() const
{
    return dim;
}


long Mask::get_count()
{
    if (dirty) {
        recount();
        dirty = false;
    }
    return count;
}
