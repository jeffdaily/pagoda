#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Dimension.H"
#include "Mask.H"

Mask::Mask(Dimension *dim)
    :   dim(dim)
    ,   need_recount(true)
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


int64_t Mask::get_size() const
{
    return get_dim()->get_size();
}


int64_t Mask::get_count()
{
    if (need_recount) {
        need_recount = false;
        recount();
    }
    return count;
}
