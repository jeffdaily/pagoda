#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>

#include <macdecls.h>

#include "Dimension.H"
#include "LocalMask.H"
#include "Slice.H"
#include "Util.H"

using std::copy;
using std::fill;


LocalMask::LocalMask(Dimension *dim)
    :   Mask(dim)
    ,   data(size, 0)
{
}


LocalMask::~LocalMask()
{
}


void LocalMask::get_data(vector<int> &ret)
{
    ret.assign(data.begin(), data.end());
}


void LocalMask::get_data(vector<int> &ret, int64_t lo, int64_t hi)
{
    ret.assign(data.begin()+lo, data.begin()+hi);
}


void LocalMask::clear()
{
    if (cleared) return;
    cleared = true;
    data.assign(size, 0);
}


void LocalMask::adjust(const DimSlice &slice)
{
    need_recount = true;

    int64_t lo;
    int64_t hi;
    int64_t step;

    slice.indices(size, lo, hi, step);
    for (int64_t i=lo; i<=hi; i+=step) {
        data.at(i) = 1;
    }
}


void LocalMask::adjust(const LatLonBox &box, Variable *lat, Variable *lon)
{
}


void LocalMask::adjust(double low, double hi, Variable *var, bool bitwise_or)
{
}


void LocalMask::recount()
{
    count = 0;
    for (int64_t i=0; i<size; ++i) {
        if (data.at(i) != 0) ++count;
    }
}


void LocalMask::reindex()
{
}

