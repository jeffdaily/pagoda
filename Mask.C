#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Dimension.H"
#include "Mask.H"
#include "Timing.H"


Mask::Mask(Dimension *dim)
    :   name(dim->get_name())
    ,   size(dim->get_size())
    ,   count(0)
    ,   need_recount(true)
    ,   cleared(false)
{
    TIMING("Mask::Mask(...)");
}


Mask::~Mask()
{
    TIMING("Mask::~Mask()");
}


string Mask::get_name() const
{
    TIMING("Mask::get_name()");
    return name;
}


int64_t Mask::get_size() const
{
    TIMING("Mask::get_size()");
    return size;
}


int64_t Mask::get_count()
{
    TIMING("Mask::get_count()");
    if (need_recount) {
        need_recount = false;
        recount();
    }
    return count;
}
