#include <algorithm>
    using std::copy;
    using std::fill;
#include <functional>
    using std::bind1st;
    using std::ptr_fun;
#include <iostream>
    using std::cerr;
    using std::cout;
    using std::endl;
#include <numeric>
    using std::accumulate;

#include <macdecls.h>

#include "Dimension.H"
#include "Mask.H"
#include "Slice.H"
#include "Util.H"


///////////////////////////////////////////////////////////////////////////////
// Mask
///////////////////////////////////////////////////////////////////////////////

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

