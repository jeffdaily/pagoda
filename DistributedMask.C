#include <algorithm>
    using std::copy;
    using std::fill;
#include <functional>
    using std::bind1st;
    using std::ptr_fun;
#include <numeric>
    using std::accumulate;

#include <macdecls.h>

#include "Dimension.H"
#include "DistributedMask.H"
#include "Pack.H"
#include "Slice.H"
#include "Util.H"
#include "Variable.H"


DistributedMask::DistributedMask(Dimension *dim, int value)
    :   Mask(dim)
    ,   handle(0)
    ,   handle_index(0)
    ,   lo(0)
    ,   hi(0)
    ,   counts(NULL)
{
    string name = dim->get_name();
    int64_t size = dim->get_size();
    handle = NGA_Create64(MT_INT, 1, &size, (char*)name.c_str(), NULL);
    NGA_Distribution64(handle, ME, &lo, &hi);
    GA_Fill(handle, &value);
    counts = new long[NPROC];
}


DistributedMask::~DistributedMask()
{
    GA_Destroy(handle);
    if (handle_index != 0) {
        GA_Destroy(handle_index);
    }
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


void DistributedMask::clear()
{
    // bail if already cleared once (a one-time operation)
    if (cleared) return;
    cleared = true;
    int ZERO = 0;
    GA_Fill(handle, &ZERO);
}


void DistributedMask::adjust(const DimSlice &slice)
{
    dirty = true;
    clear();

    // bail if we don't own any of the data
    if (0 > lo || 0 > hi) return;

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


void DistributedMask::adjust(const LatLonBox &box, Variable *lat, Variable *lon)
{
    dirty = true;
    clear();

    // bail if we don't own any of the data
    if (0 > lo || 0 > hi) return;

    // currently assumes the Variables are 1-D (coorindate) variables
    // which means they have the same data distribution as this mask
    int *mask_data;
    void *lat_data;
    void *lon_data;
    int lat_handle = lat->get_handle();
    int lon_handle = lon->get_handle();
    int type;
    int ndim;
    int64_t dims[1];
    int64_t size = hi-lo+1;

    lat->read();
    lon->read();
    NGA_Inquire64(lat_handle, &type, &ndim, dims);
    NGA_Access64(handle, &lo, &hi, &mask_data, NULL);
    NGA_Access64(lat_handle, &lo, &hi, &lat_data, NULL);
    NGA_Access64(lon_handle, &lo, &hi, &lon_data, NULL);

    switch (type) {
#define adjust_op(MTYPE,type) \
        case MTYPE: { \
            type *plat = (type*)lat_data; \
            type *plon = (type*)lon_data; \
            for (int64_t i=0; i<size; ++i) { \
                mask_data[i] = box.contains(plat[i], plon[i]) ? 1 : 0; \
            } \
            break; \
        }
        adjust_op(MT_INT,int)
        adjust_op(MT_LONGINT,long int)
        adjust_op(MT_REAL,float)
        adjust_op(MT_DBL,double)
#undef adjust_op
    }

    NGA_Release64(lon_handle, &lo, &hi);
    NGA_Release64(lat_handle, &lo, &hi);
    NGA_Release_update64(handle, &lo, &hi);
    lat->release_handle();
    lon->release_handle();
}


void DistributedMask::adjust(
        double min,
        double max,
        Variable *var,
        bool bitwise_or)
{
    dirty = true;
    clear();

    // bail if we don't own any of the data
    if (0 > lo || 0 > hi) return;

    // currently assumes the Variable is a 1-D (coorindate) variable 
    // which means it has the same data distribution as this mask
    int *mask;
    void *data;
    int var_handle = var->get_handle();
    int type;
    int ndim;
    int64_t dims[1];

    NGA_Inquire64(var_handle, &type, &ndim, dims);
    var->read();
    NGA_Access64(handle, &lo, &hi, &mask, NULL);
    NGA_Access64(var_handle, &lo, &hi, &data, NULL);

    switch (type) {
#define adjust_op(MTYPE,type) \
        case MTYPE: { \
            type *pdata = (type*)data; \
            if (bitwise_or) { \
                for (int64_t i=0,limit=hi-lo+1; i<limit; ++i) { \
                    mask[i] |= (pdata[i] >= lo && pdata[i] <= hi); \
                } \
            } else { \
                for (int64_t i=0,limit=hi-lo+1; i<limit; ++i) { \
                    mask[i] &= (pdata[i] >= lo && pdata[i] <= hi); \
                } \
            } \
            break; \
        }
        adjust_op(MT_INT,int)
        adjust_op(MT_LONGINT,long int)
        adjust_op(MT_REAL,float)
        adjust_op(MT_DBL,double)
#undef adjust_op
    }

    NGA_Release64(var_handle, &lo, &hi);
    NGA_Release_update64(handle, &lo, &hi);
    var->release_handle();
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


void DistributedMask::reindex()
{
    if (0 == handle_index) {
        int type;
        int ndim;
        int64_t dims;

        NGA_Inquire64(handle, &type, &ndim, &dims);
        handle_index = NGA_Create64(MT_INT, 1, &dims, "index", NULL);
    }
    int ONE_NEG = -1;
    int handle_tmp;
    int64_t tmp_count = get_count();
    int64_t icount;

    GA_Fill(handle_index, &ONE_NEG);
    handle_tmp = NGA_Create64(MT_INT, 1, &tmp_count, "tmp_enum", NULL);
    //GA_Patch_enum64(handle_tmp, 0, tmp_count-1, 0, 1);
    enumerate(handle_tmp, 0, 1);
    //GA_Unpack64(handle_tmp, handle_index, handle, 0, tmp_count-1, &icount);
    unpack1d(handle_tmp, handle_index, handle);
    GA_Destroy(handle_tmp);
    /*
    if (GA_Nnodes() == 1) {
        int64_t ZERO = 0;
        int64_t bighi = dim->get_size()-1;
        int64_t smlhi = tmp_count-1;
        int *src,*dst,*mask;
        NGA_Access64(handle_index, &ZERO, &bighi, &dst, NULL);
        NGA_Access64(handle, &ZERO, &bighi, &mask, NULL);
        NGA_Access64(handle_tmp, &ZERO, &smlhi, &src, NULL);
        for (int64_t i=0; i<=bighi; ++i) {
            std::cout << "[" << i << "]" << dst[i] << " " << mask[i] << std::endl;
        }
    }
    */
}

