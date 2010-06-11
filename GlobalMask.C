#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <numeric>
#include <vector>

#include <ga.h>
#include <macdecls.h>
#include <message.h>

#include "Array.H"
#include "Common.H"
#include "Debug.H"
#include "Dimension.H"
#include "GlobalArray.H"
#include "GlobalMask.H"
#include "Pack.H"
#include "Slice.H"
#include "Timing.H"
#include "Variable.H"

using std::accumulate;
using std::vector;


/**
 * Construct GlobalMask based on the size of the given Dimension.
 *
 * @param dim Dimension to base size on
 */
GlobalMask::GlobalMask(Dimension *dim)
    :   Mask()
    ,   mask(NULL)
{
    TIMING("GlobalMask::GlobalMask(Dimension*)");
    TRACER("GlobalMask ctor size=%ld\n", dim->get_size());

    mask = new GlobalArray(C_INT, vector<int64_t>(1,dim->get_size()));
    mask->fill(int(1));
}


/**
 * Copy constructor.
 *
 * @param that other GlobalMask
 */
GlobalMask::GlobalMask(const GlobalMask &that)
    :   Mask()
    ,   mask(NULL)
{
    TIMING("GlobalMask::GlobalMask(GlobalMask)");
    TRACER("GlobalMask ctor copy\n");

    mask = new GlobalArray(*mask);
}


/**
 * Destructor.
 */
GlobalMask::~GlobalMask()
{
    TIMING("GlobalMask::~GlobalMask()");

    delete mask;
}


/**
 * Returns the number of bits in the mask, regardless of whether any are set.
 *
 * @return the number of bits
 */
int64_t GlobalMask::get_size() const
{
    TIMING("GlobalMask::get_size()");

    return mask->get_shape().at(0);
}


/**
 * Returns the number of set bits in the mask.
 *
 * A bit is set if it is any value except zero.
 *
 * @return the number of set bits
 */
int64_t GlobalMask::get_count() const
{
    int64_t ZERO = 0;
    vector<int64_t> counts(GA_Nnodes());

    TIMING("GlobalMask::get_count()");

    std::fill(counts.begin(), counts.end(), ZERO);
    if (mask->owns_data()) {
        int *data = (int*)mask->access();
        for (int64_t i=0,limit=mask->get_local_size(); i<limit; ++i) {
            if (data[i] != 0) {
                ++(counts[ME]);
            }
        }
        mask->release();
    }
#if SIZEOF_INT64_T == SIZEOF_INT
    armci_msg_igop(&counts[0], NPROC, "+");
#elif SIZEOF_INT64_T == SIZEOF_LONG
    armci_msg_lgop(&counts[0], NPROC, "+");
#elif SIZEOF_INT64_T == SIZEOF_LONG_LONG
    armci_msg_llgop(&counts[0], NPROC, "+");
#else
#   error SIZEOF_INT64_T == ???
#endif
    return accumulate(counts.begin(), counts.end(), ZERO);
}


/**
 * Sets all bits to 0.
 */
void GlobalMask::clear()
{
    TIMING("GlobalMask::clear()");

    mask->fill(int(0));
}


/**
 * Sets all bits to 1.
 */
void GlobalMask::fill()
{
    TIMING("GlobalMask::fill()");

    mask->fill(int(1));
}


/**
 * Set bits based on the given slice notation.
 *
 * @param slice the dimension slice
 */
void GlobalMask::modify(const DimSlice &slice)
{
    int64_t slo;
    int64_t shi;
    int64_t step;
    int *data;
    vector<int64_t> lo;
    vector<int64_t> hi;

    TIMING("GlobalMask::modify(DimSlice)");

    // bail if mask doesn't own any of the data
    if (!mask->owns_data()) return;

    slice.indices(get_size(), slo, shi, step);
    mask->get_distribution(lo,hi);
    data = (int*)mask->access();
    if (lo[0] <= slo) {
        int64_t start = slo-lo[0];
        if (hi[0] >= shi) {
            for (int64_t i=start,limit=(shi-lo[0]); i<limit; i+=step) {
                data[i] = 1;
            }
        } else { // hi < shi
            for (int64_t i=start,limit=(hi[0]-lo[0]+1); i<limit; i+=step) {
                data[i] = 1;
            }
        }
    } else if (lo[0] <= shi) { // && lo[0] > slo
        int64_t start = slo-lo[0];
        while (start < 0) start+=step;
        if (hi[0] >= shi) {
            for (int64_t i=start,limit=(shi-lo[0]); i<limit; i+=step) {
                data[i] = 1;
            }
        } else { // hi < shi
            for (int64_t i=start,limit=(hi[0]-lo[0]+1); i<limit; i+=step) {
                data[i] = 1;
            }
        }
    }
    mask->release_update();
}


void GlobalMask::modify(const LatLonBox &box, const Array *lat, const Array *lon)
{
    int *mask_data;
    void *lat_data;
    void *lon_data;
    int64_t size = mask->get_local_size();

    TIMING("GlobalMask::modify(LatLonBox,Array*,Array*)\n");

    // bail if we don't own any of the data
    if (!mask->owns_data()) return;

    // lat, lon, and this must have the same shape
    // but it is assumed that they have the same distributions
    if (lat->get_shape() != lon->get_shape()) {
        GA_Error("GlobalMask::modify lat lon shape mismatch", 0);
    } else if (mask->get_shape() != lat->get_shape()) {
        GA_Error("GlobalMask::modify mask lat/lon shape mismatch", 0);
    } else if (lat->get_type() != lon->get_type()) {
        GA_Error("GlobalMask::modify lat lon types differ", 0);
    }

    mask_data = (int*)mask->access();
    lat_data = lat->access();
    lon_data = lon->access();

    switch (lat->get_type().as_ma()) {
#define adjust_op(MTYPE,TYPE) \
        case MTYPE: { \
            TYPE *plat = (TYPE*)lat_data; \
            TYPE *plon = (TYPE*)lon_data; \
            for (int64_t i=0; i<size; ++i) { \
                if (box.contains(plat[i], plon[i])) { \
                    mask_data[i] = 1; \
                } \
            } \
            break; \
        }
        adjust_op(C_INT,int)
        adjust_op(C_LONG,long)
        adjust_op(C_LONGLONG,long long)
        adjust_op(C_FLOAT,float)
        adjust_op(C_DBL,double)
        adjust_op(C_LDBL,long double)
#undef adjust_op
    }

    mask->release_update();
    lat->release();
    lon->release();
}


void GlobalMask::modify(double min, double max, const Array *var)
{
    TIMING("GlobalMask::modify(double,double,Variable*,bool)");

    int *mask_data;
    void *var_data;

    // bail if we don't own any of the data
    if (!mask->owns_data()) return;

    // lat, lon, and this must have the same shape
    // but it is assumed that they have the same distributions
    if (mask->get_shape() != var->get_shape()) {
        GA_Error("GlobalMask::modify mask var shape mismatch", 0);
    }

    mask_data = (int*)mask->access();
    var_data = var->access();

    switch (var->get_type().as_ma()) {
#define adjust_op(MTYPE,TYPE) \
        case MTYPE: { \
            TYPE *pdata = (TYPE*)var_data; \
            for (int64_t i=0,limit=mask->get_local_size(); i<limit; ++i) { \
                if (pdata[i] >= min && pdata[i] <= max) { \
                    mask_data[i] = 1; \
                } \
            } \
            break; \
        }
        adjust_op(C_INT,int)
        adjust_op(C_LONG,long)
        adjust_op(C_LONGLONG,long long)
        adjust_op(C_FLOAT,float)
        adjust_op(C_DBL,double)
        adjust_op(C_LDBL,long double)
#undef adjust_op
    }

    mask->release_update();
    var->release();
}


/**
 * Change any non-zero value to 1 within this GlobalMask.
 */
void GlobalMask::normalize()
{

}


/**
 * Return this Mask as an Array.
 *
 * In this case, return the underlying GlobalArray used as storage for this
 * GlobalMask.
 *
 * @return the GlobalArray used as storage for this GlobalMask
 */
Array* GlobalMask::as_array() const
{
    return mask;
}


/**
 * Return a new Array, enumerating each non-zero bit stating from zero.
 *
 * A new Array is created based on the size of this GlobalMask.  Then, based on the
 * set bits of this GlobalMask, the new Array is enumerated such the first set bit
 * is 0, the next set bit is 1, and so on.  Any unset bit is set to the value
 * of -1.  
 *
 * For example:
 *
 * GlobalMask:  0  0  0  0  1  1  1  0  1  2 -1  0  1
 * Ret:  -1 -1 -1 -1  0  1  2 -1  3  4  5 -1  6
 *
 * @return the enumerated, reindexed Array
 */
Array* GlobalMask::reindex() const
{
    TIMING("GlobalMask::reindex()");
    TRACER("GlobalMask::reindex() BEGIN\n");

    int64_t count = 0;
    vector<int64_t> size(1);
    Array *ret;
    int handle_tmp;

    count = get_count();
    size.assign(1, get_size());
    ret = new GlobalArray(C_INT, size);
    ret->fill(-1);

    handle_tmp = NGA_Create64(C_INT, 1, &count, "tmp_enum", NULL);
    enumerate(handle_tmp, NULL, NULL);
    unpack1d(handle_tmp, ret->handle, handle);
    GA_Destroy(handle_tmp);
    /*
    if (GA_Nnodes() == 1) {
        int64_t ZERO = 0;
        int64_t bighi = size-1;
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
    TRACER("GlobalMask::reindex() END\n");

    return ret;
}
