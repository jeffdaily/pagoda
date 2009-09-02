#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
//#include <functional>
//#include <numeric>

#include <macdecls.h>

#include "Dimension.H"
#include "LocalMask.H"
#include "Slice.H"
#include "Util.H"

//using std::accumulate;
//using std::bind1st;
using std::copy;
using std::fill;
//using std::ptr_fun;


LocalMask::LocalMask(Dimension *dim)
    :   Mask(dim)
    ,   data(NULL)
    ,   index(NULL)
{
    data = new int[dim->get_size()];
    fill(data, data+dim->get_size(), 0);
}


LocalMask::~LocalMask()
{
}


int* LocalMask::get_data() const
{
    int64_t size = dim->get_size();
    int *ret = new int[size];
    copy(data, data+size, ret);
    return ret;
}


int* LocalMask::get_data(int64_t lo, int64_t hi)
{
    int *ret = new int[hi-lo+1];
    copy(data+lo, data+hi, ret);
    return ret;
}


void LocalMask::clear()
{
    if (cleared) return;
    cleared = true;
    fill(data, data+dim->get_size(), 0);
}


void LocalMask::adjust(const DimSlice &slice)
{
    dirty = true;

    int64_t size = dim->get_size();
    int64_t lo;
    int64_t hi;
    int64_t step;

    slice.indices(size, lo, hi, step);
    for (int64_t i=lo; i<=hi; i+=step) {
        data[i] = 1;
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
    for (int64_t i=0,limit=dim->get_size(); i<limit; ++i) {
        if (data[i] != 0) ++count;
    }
}


void LocalMask::reindex()
{
}

