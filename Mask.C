#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Dimension.H"
#include "Mask.H"

Mask::Mask(Dimension *dim)
    :   name(dim->get_name())
    ,   size(dim->get_size())
    ,   count(0)
    ,   need_recount(true)
    ,   cleared(false)
{
}


Mask::~Mask()
{
}


string Mask::get_name() const
{
    return name;
}


int64_t Mask::get_size() const
{
    return size;
}


int64_t Mask::get_count()
{
    if (need_recount) {
        need_recount = false;
        recount();
    }
    return count;
}
