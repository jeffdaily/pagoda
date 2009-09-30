#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>

#include <macdecls.h>

#include "Dimension.H"
#include "LocalMask.H"
#include "Slice.H"
#include "Timing.H"
#include "Util.H"

using std::copy;
using std::fill;


LocalMask::LocalMask(Dimension *dim)
    :   Mask(dim)
    ,   data(size, 0)
{
    TIMING("LocalMask::LocalMask(Dimension*)");
}


LocalMask::~LocalMask()
{
    TIMING("LocalMask::~LocalMask");
}


void LocalMask::get_data(vector<int> &ret)
{
    TIMING("LocalMask::get_data(vector<int>)");
    ret.assign(data.begin(), data.end());
}


void LocalMask::get_data(vector<int> &ret, int64_t lo, int64_t hi)
{
    TIMING("LocalMask::get_data(vector<int>,int64_t,int64_t)");
    ret.assign(data.begin()+lo, data.begin()+hi);
}


void LocalMask::clear()
{
    TIMING("LocalMask::clear()");
    if (cleared) return;
    cleared = true;
    data.assign(size, 0);
}


void LocalMask::adjust(const DimSlice &slice)
{
    TIMING("LocalMask::adjust(DimSlice)");
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
    TIMING("LocalMask::adjust(LatLonBox,Variable*,Variable*)");
}


void LocalMask::adjust(double low, double hi, Variable *var, bool bitwise_or)
{
    TIMING("LocalMask::adjust(double,double,Variable*,bool)");
}


void LocalMask::recount()
{
    TIMING("LocalMask::recount()");
    count = 0;
    for (int64_t i=0; i<size; ++i) {
        if (data.at(i) != 0) ++count;
    }
}


void LocalMask::reindex()
{
    TIMING("LocalMask::reindex()");
}
