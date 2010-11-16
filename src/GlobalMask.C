#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <numeric>
#include <vector>

#include <ga.h>
#include <macdecls.h>

#include "Array.H"
#include "Common.H"
#include "Debug.H"
#include "Dimension.H"
#include "GlobalArray.H"
#include "GlobalMask.H"
#include "NotImplementedException.H"
#include "Pack.H"
#include "Slice.H"
#include "Timing.H"
#include "Util.H"
#include "Variable.H"

using std::accumulate;
using std::vector;


/**
 * Construct GlobalMask based on the size of the given Dimension.
 *
 * @param[in] dim Dimension to base size on
 */
GlobalMask::GlobalMask(const Dimension *dim)
    :   Mask()
    ,   mask(NULL)
    ,   index(NULL)
    ,   sum(NULL)
    ,   count(-1)
    ,   last_excl(false)
    ,   dirty_index(true)
    ,   dirty_sum(true)
    ,   dirty_count(true)
    ,   name(dim->get_name())
{
    TIMING("GlobalMask::GlobalMask(Dimension*)");
    TRACER("GlobalMask ctor size=%ld\n", dim->get_size());

    mask = new GlobalArray(C_INT, vector<int64_t>(1,dim->get_size()));
    mask->fill_value(1);
}


/**
 * Copy constructor.
 *
 * @param[in] that other GlobalMask
 */
GlobalMask::GlobalMask(const GlobalMask &that)
    :   Mask()
    ,   mask(NULL)
    ,   index(NULL)
    ,   sum(NULL)
    ,   count(-1)
    ,   last_excl(false)
    ,   dirty_index(true)
    ,   dirty_sum(true)
    ,   dirty_count(true)
    ,   name(that.name)
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
    delete index;
    delete sum;
}


string GlobalMask::get_name() const
{
    return name;
}


/**
 * Returns the number of set bits in the mask.
 *
 * A bit is set if it is any value except zero.
 *
 * @return the number of set bits
 */
int64_t GlobalMask::get_count()
{
    TIMING("GlobalMask::get_count()");

    if (dirty_count) {
        int64_t ZERO = 0;
        vector<int64_t> counts(pagoda::num_nodes(), ZERO);

        if (owns_data()) {
            int *data = (int*)access();
            for (int64_t i=0,limit=get_local_size(); i<limit; ++i) {
                if (data[i] != 0) {
                    ++(counts[pagoda::nodeid()]);
                }
            }
            release();
        }
        pagoda::gop_sum(counts);
        count = accumulate(counts.begin(), counts.end(), ZERO);
        dirty_count = false;
    }

    return count;
}


/**
 * Sets all bits to 0.
 */
void GlobalMask::clear()
{
    TIMING("GlobalMask::clear()");

    mask->fill_value(0);
}


/**
 * Sets all bits to 1.
 */
void GlobalMask::reset()
{
    TIMING("GlobalMask::reset()");

    mask->fill_value(1);
}


/**
 * Set bits based on the given slice notation.
 *
 * @param[in] slice the dimension slice
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

    dirty_index = true; // aggresive dirtiness
    dirty_sum = true;
    dirty_count = true;

    // bail if mask doesn't own any of the data
    if (!owns_data()) {
        return;
    }

    slice.indices(get_size(), slo, shi, step);
    get_distribution(lo,hi);
    data = (int*)access();
    if (lo[0] <= slo) {
        int64_t start = slo-lo[0];
        if (hi[0] >= shi) {
            for (int64_t i=start,limit=(shi-lo[0]); i<=limit; i+=step) {
                data[i] = 1;
            }
        }
        else {   // hi < shi
            for (int64_t i=start,limit=(hi[0]-lo[0]+1); i<=limit; i+=step) {
                data[i] = 1;
            }
        }
    }
    else if (lo[0] <= shi) {   // && lo[0] > slo
        int64_t start = slo-lo[0];
        while (start < 0) {
            start+=step;
        }
        if (hi[0] >= shi) {
            for (int64_t i=start,limit=(shi-lo[0]); i<=limit; i+=step) {
                data[i] = 1;
            }
        }
        else {   // hi < shi
            for (int64_t i=start,limit=(hi[0]-lo[0]+1); i<=limit; i+=step) {
                data[i] = 1;
            }
        }
    }
    release_update();
}


void GlobalMask::modify(const LatLonBox &box, const Array *lat, const Array *lon)
{
    int *mask_data;
    void *lat_data;
    void *lon_data;
    int64_t size = get_local_size();
    DataType type = lat->get_type();

    TIMING("GlobalMask::modify(LatLonBox,Array*,Array*)\n");

    dirty_index = true; // aggresive dirtiness
    dirty_sum = true;
    dirty_count = true;

    // bail if we don't own any of the data
    if (!owns_data()) {
        return;
    }

    // lat, lon, and this must have the same shape
    // but it is assumed that they have the same distributions
    if (lat->get_shape() != lon->get_shape()) {
        pagoda::abort("GlobalMask::modify lat lon shape mismatch", 0);
    }
    else if (get_shape() != lat->get_shape()) {
        pagoda::abort("GlobalMask::modify mask lat/lon shape mismatch", 0);
    }
    else if (lat->get_type() != lon->get_type()) {
        pagoda::abort("GlobalMask::modify lat lon types differ", 0);
    }

    mask_data = (int*)access();
    lat_data = lat->access();
    lon_data = lon->access();

#define adjust_op(DTYPE,TYPE) \
    if (type == DTYPE) { \
        TYPE *plat = (TYPE*)lat_data; \
        TYPE *plon = (TYPE*)lon_data; \
        for (int64_t i=0; i<size; ++i) { \
            if (box.contains(plat[i], plon[i])) { \
                mask_data[i] = 1; \
            } \
        } \
    } else
    adjust_op(DataType::INT,int)
    adjust_op(DataType::LONG,long)
    adjust_op(DataType::LONGLONG,long long)
    adjust_op(DataType::FLOAT,float)
    adjust_op(DataType::DOUBLE,double)
    adjust_op(DataType::LONGDOUBLE,long double) {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
#undef adjust_op

    release_update();
    lat->release();
    lon->release();
}


void GlobalMask::modify(double min, double max, const Array *var)
{
    int *mask_data;
    void *var_data;
    DataType type = var->get_type();

    TIMING("GlobalMask::modify(double,double,Variable*,bool)");

    dirty_index = true; // aggresive dirtiness
    dirty_sum = true;
    dirty_count = true;

    // bail if we don't own any of the data
    if (!owns_data()) {
        return;
    }

    // lat, lon, and this must have the same shape
    // but it is assumed that they have the same distributions
    if (get_shape() != var->get_shape()) {
        pagoda::abort("GlobalMask::modify mask var shape mismatch", 0);
    }

    mask_data = (int*)access();
    var_data = var->access();

#define adjust_op(DTYPE,TYPE) \
    if (type == DTYPE) { \
        TYPE *pdata = (TYPE*)var_data; \
        for (int64_t i=0,limit=get_local_size(); i<limit; ++i) { \
            if (pdata[i] >= min && pdata[i] <= max) { \
                mask_data[i] = 1; \
            } \
        } \
    } else
    adjust_op(DataType::INT,int)
    adjust_op(DataType::LONG,long)
    adjust_op(DataType::LONGLONG,long long)
    adjust_op(DataType::FLOAT,float)
    adjust_op(DataType::DOUBLE,double)
    adjust_op(DataType::LONGDOUBLE,long double) {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
#undef adjust_op

    release_update();
    var->release();
}


/**
 * Change any non-zero value to 1 within this GlobalMask.
 */
void GlobalMask::normalize()
{
    TIMING("GlobalMask::normalize()");
    TRACER("GlobalMask::normalize()");

    if (owns_data()) {
        int *mask_data = (int*)access();
        for (int64_t i=0,limit=get_local_size(); i<limit; ++i) {
            if (mask_data[i] != 0) {
                mask_data[i] = 1;
            }
        }
        release_update();
    }
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
 * Mask:   0  0  0  0  1  1  1  0  1  2 -1  0  1
 * Rtrn:  -1 -1 -1 -1  0  1  2 -1  3  4  5 -1  6
 *
 * @return the enumerated, reindexed Array
 */
Array* GlobalMask::reindex()
{
    vector<int64_t> count(1, get_count());
    vector<int64_t> size(1, get_size());
    Array *tmp;

    TIMING("GlobalMask::reindex()");
    TRACER("GlobalMask::reindex() BEGIN\n");

    if (dirty_index) {
        if (!index) {
            index = new GlobalArray(C_INT, size);
        }

        if (count[0] > 0) {
            index->fill_value(-1);
            tmp = new GlobalArray(C_INT, count);
            pagoda::enumerate(tmp, NULL, NULL);
            pagoda::unpack1d(tmp, index, mask);
            delete tmp;
        }
        else {
            pagoda::enumerate(index, NULL, NULL);
        }
        dirty_index = false;
    }

    TRACER("GlobalMask::reindex() END\n");

    return index;
}


Array* GlobalMask::partial_sum(bool excl)
{

    TIMING("GlobalMask::partial_sum(bool)");

    if (last_excl != excl) {
        dirty_sum = true;
    }
    last_excl = excl;

    if (dirty_sum) {
        if (!sum) {
            sum = Array::create(get_type(), get_shape());
        }
        pagoda::partial_sum(this, sum, excl);
        dirty_sum = false;
    }

    return sum;
}


// ########################################
// BEGIN FORWARDS FOR Array IMPLEMENTATION
// ########################################

DataType GlobalMask::get_type() const
{
    return mask->get_type();
}


vector<int64_t> GlobalMask::get_shape() const
{
    return mask->get_shape();
}


vector<int64_t> GlobalMask::get_local_shape() const
{
    return mask->get_local_shape();
}


int64_t GlobalMask::get_ndim() const
{
    return mask->get_ndim();
}


void GlobalMask::fill(void *value)
{
    mask->fill(value);
}


void GlobalMask::copy(const Array *src)
{
    mask->copy(src);
}


void GlobalMask::copy(const Array *src, const vector<int64_t> &src_lo, const vector<int64_t> &src_hi, const vector<int64_t> &dst_lo, const vector<int64_t> &dst_hi)
{
    mask->copy(src,src_lo,src_hi,dst_lo,dst_hi);
}


bool GlobalMask::same_distribution(const Array *other) const
{
    return mask->same_distribution(other);
}


bool GlobalMask::owns_data() const
{
    return mask->owns_data();
}


void GlobalMask::get_distribution(
    vector<int64_t> &lo, vector<int64_t> &hi) const
{
    mask->get_distribution(lo,hi);
}


void* GlobalMask::access()
{
    return mask->access();
}


void* GlobalMask::access() const
{
    return mask->access();
}


void GlobalMask::release() const
{
    mask->release();
}


void GlobalMask::release_update()
{
    mask->release_update();
}


void* GlobalMask::get(void *buffer) const
{
    return mask->get(buffer);
}


void* GlobalMask::get(int64_t lo, int64_t hi, void *buffer) const
{
    return mask->get(lo,hi,buffer);
}


void* GlobalMask::get(const vector<int64_t> &lo, const vector<int64_t> &hi,
                      void *buffer) const
{
    return mask->get(lo,hi,buffer);
}


void GlobalMask::put(void *buffer)
{
    mask->put(buffer);
}


void GlobalMask::put(void *buffer, int64_t lo, int64_t hi)
{
    mask->put(buffer,lo,hi);
}


void GlobalMask::put(void *buffer,
                     const vector<int64_t> &lo, const vector<int64_t> &hi)
{
    mask->put(buffer,lo,hi);
}


void GlobalMask::scatter(void *buffer, vector<int64_t> &subscripts)
{
    mask->scatter(buffer,subscripts);
}


void* GlobalMask::gather(vector<int64_t> &subscripts, void *buffer) const
{
    return mask->gather(subscripts, buffer);
}


Array* GlobalMask::add(const Array *rhs) const
{
    return mask->add(rhs);
}


Array* GlobalMask::iadd(const Array *rhs)
{
    return mask->iadd(rhs);
}


Array* GlobalMask::sub(const Array *rhs) const
{
    return mask->sub(rhs);
}


Array* GlobalMask::isub(const Array *rhs)
{
    return mask->isub(rhs);
}


Array* GlobalMask::mul(const Array *rhs) const
{
    return mask->mul(rhs);
}


Array* GlobalMask::imul(const Array *rhs)
{
    return mask->imul(rhs);
}


Array* GlobalMask::div(const Array *rhs) const
{
    return mask->div(rhs);
}


Array* GlobalMask::idiv(const Array *rhs)
{
    return mask->idiv(rhs);
}


Array* GlobalMask::max(const Array *rhs) const
{
    return mask->max(rhs);
}


Array* GlobalMask::imax(const Array *rhs)
{
    return mask->imax(rhs);
}


Array* GlobalMask::min(const Array *rhs) const
{
    return mask->min(rhs);
}


Array* GlobalMask::imin(const Array *rhs)
{
    return mask->imin(rhs);
}


Array* GlobalMask::pow(double exponent) const
{
    return mask->pow(exponent);
}


Array* GlobalMask::ipow(double exponent)
{
    return mask->ipow(exponent);
}


ostream& GlobalMask::print(ostream &os) const
{
    os << "GlobalMask";
    return os;
}


void GlobalMask::dump() const
{
    mask->dump();
}
