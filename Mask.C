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
    ,   dirty(false)
    ,   count(0)
{
}


Mask::Mask(const Mask &other)
    :   dim(other.dim)
    ,   dirty(other.dirty)
    ,   count(other.count)
{
}


Mask& Mask::operator = (const Mask &other)
{
    if (&other != this) {
        dim = other.dim;
        dirty = other.dirty;
        count = other.count;
    }
    return *this;
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



///////////////////////////////////////////////////////////////////////////////
// LocalMask
///////////////////////////////////////////////////////////////////////////////

LocalMask::LocalMask(Dimension *dim)
    :   Mask(dim)
    ,   data(NULL)
{
    data = new int[dim->get_size()];
    fill(data, data+dim->get_size(), 0);
}


LocalMask::LocalMask(const LocalMask &other)
    :   Mask(other)
    ,   data(other.data)
{
}


LocalMask& LocalMask::operator = (const LocalMask &other)
{
    if (&other != this) {
        this->Mask::operator=(other);
        data = other.data;
    }
    return *this;
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


void LocalMask::recount()
{
    count = 0;
    for (int64_t i=0,limit=dim->get_size(); i<limit; ++i) {
        if (data[i] != 0) ++count;
    }
}



///////////////////////////////////////////////////////////////////////////////
// DistributedMask
///////////////////////////////////////////////////////////////////////////////

DistributedMask::DistributedMask(Dimension *dim)
    :   Mask(dim)
    ,   handle(0)
    ,   lo(0)
    ,   hi(0)
    ,   counts(NULL)
{
    string name = dim->get_name();
    int64_t size = dim->get_size();
    handle = NGA_Create64(MT_INT, 1, &size, (char*)name.c_str(), NULL);
    int zero = 0;
    NGA_Distribution64(handle, ME, &lo, &hi);
    GA_Fill(handle, &zero);
    counts = new long[NPROC];
}


DistributedMask::DistributedMask(const DistributedMask &other)
    :   Mask(other)
    ,   handle(other.handle)
    ,   lo(other.lo)
    ,   hi(other.hi)
    ,   counts(other.counts)
{
}


DistributedMask& DistributedMask::operator = (const DistributedMask &other)
{
    if (&other != this) {
        this->Mask::operator=(other);
        handle = other.handle;
        lo = other.lo;
        hi = other.hi;
        counts = other.counts;
    }
    return *this;
}


DistributedMask::~DistributedMask()
{
}


void DistributedMask::recount()
{
    int *data;
    count = 0;
    fill(counts, counts+NPROC, 0);
    NGA_Access64(handle, &lo, &hi, &data, NULL);
    for (int64_t i=0,limit=(hi-lo+1); i<limit; ++i) {
        if (data[i] != 0) ++count;
    }
    NGA_Release_update64(handle, &lo, &hi);
    counts[ME] = count;
    GA_Lgop(counts, NPROC, "+");
    count = accumulate(counts, counts+NPROC, 0);
}


int* DistributedMask::get_data() const
{
    int64_t size = dim->get_size();
    int *data = new int[size];
    int64_t glo = 0;
    int64_t ghi = size - 1;
    NGA_Get64(handle, &glo, &ghi, data, NULL);
    return data;
}


int* DistributedMask::get_data(int64_t lo, int64_t hi)
{
    int *data = new int[hi-lo+1];
    NGA_Get64(handle, &lo, &hi, data, NULL);
    return data;
}


void DistributedMask::adjust(const DimSlice &slice)
{
    dirty = true;

    int64_t size = dim->get_size();
    int64_t slo;
    int64_t shi;
    int64_t step;
    int *data;

    slice.indices(size, slo, shi, step);
    NGA_Access64(handle, &lo, &hi, &data, NULL);
    if (lo <= slo) {
        int64_t start = slo-lo;
        if (hi >= shi) {
            for (int64_t i=start,limit=(shi-lo); i<limit; i+=step) {
                data[i] = 1;
            }
        } else { // hi < shi
            for (int64_t i=start,limit=(hi-lo+1); i<limit; i+=step) {
                data[i] = 1;
            }
        }
    } else if (lo <= shi) { // && lo > slo
        int64_t start = slo-lo;
        while (start < 0) start+=step;
        if (hi >= shi) {
            for (int64_t i=start,limit=(shi-lo); i<limit; i+=step) {
                data[i] = 1;
            }
        } else { // hi < shi
            for (int64_t i=start,limit=(hi-lo+1); i<limit; i+=step) {
                data[i] = 1;
            }
        }
    }
    NGA_Release_update64(handle, &lo, &hi);
}

