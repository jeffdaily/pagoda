#include <algorithm>
#include <functional>
#include <numeric>

using std::accumulate;
using std::copy;
using std::multiplies;

#include <ga.h>

#include "Array.H"

template <class InputIterator1, class Value>
static bool less_than(InputIterator1 first1, InputIterator1 last1, Value value)
{
    while (first1!=last1) {
        if (*first1 >= value) {
            return false;
        }
        ++first1;
    }
    return true;
}


template <class InputIterator1, class InputIterator2>
static void cast(InputIterator1 first, InputIterator1 last, InputIterator2 result)
{
    while (first!=last) {
        *result++ = *first++;
    }
}


Array::Array(DataType type, vector<int64_t> shape)
    :   handle(0)
    ,   type(type)
    ,   shape(shape)
    ,   lo()
    ,   hi()
{
    handle = NGA_Create64(type, shape.size(), &shape[0], "noname", NULL);
    get_distribution();
}


Array::Array(DataType type, vector<Dimension*> dims)
    :   handle(0)
    ,   type(type)
    ,   shape()
    ,   lo()
    ,   hi()
{
    for (vector<Dimension*>::const_iterator it=dims.begin(), end=dims.end();
            it!=end; ++it) {
        shape.push_back((*it)->get_size());
    }
    handle = NGA_Create64(type, shape.size(), &shape[0], "noname", NULL);
    get_distribution();
}


Array::Array(const Array &that)
    :   handle(0)
    ,   type(that.type)
    ,   shape(that.shape)
    ,   lo(that.lo)
    ,   hi(that.hi)
{
    handle = GA_Duplicate(that.handle, "noname");
}


Array::~Array()
{
    GA_Destroy(handle);
}


DataType Array::get_type() const
{
    return type;
}


vector<int64_t> Array::get_shape() const
{
    return shape;
}


size_t Array::get_ndim() const
{
    return shape.size();
}


Array& Array::operator=(const Array &that)
{
    if (this == &that) {
        return *this;
    }

    if (type == that.type && shape == that.shape) {
        // distributions are assumed to be identical
        GA_Copy(that.handle, handle);
    } else if (shape == that.shape) {
        // distributions are assumed to be identical
        // types are different, so this is a cast
        if (has_data()) {
            vector<int64_t> ld(lo.size());
            void *this_data;
            void *that_data;
            NGA_Access64(handle,      &lo[0], &hi[0], &this_data, &ld[0]);
            NGA_Access64(that.handle, &lo[0], &hi[0], &that_data, &ld[0]);
#define cast_helper(src_mt,src_t,dst_mt,dst_t) \
            if (type == dst_mt && that.type == src_mt) { \
                dst_t *dst = (dst_t*)this_data; \
                src_t *src = (src_t*)that_data; \
                cast(dst,dst+get_count(),src); \
            } else
            cast_helper(MT_C_INT,     int,        MT_C_INT,int)
            cast_helper(MT_C_LONGINT, long,       MT_C_INT,int)
            cast_helper(MT_C_LONGLONG,long long,  MT_C_INT,int)
            cast_helper(MT_C_FLOAT,   float,      MT_C_INT,int)
            cast_helper(MT_C_DBL,     double,     MT_C_INT,int)
            cast_helper(MT_C_LDBL,    long double,MT_C_INT,int)
            cast_helper(MT_C_INT,     int,        MT_C_LONGINT,long)
            cast_helper(MT_C_LONGINT, long,       MT_C_LONGINT,long)
            cast_helper(MT_C_LONGLONG,long long,  MT_C_LONGINT,long)
            cast_helper(MT_C_FLOAT,   float,      MT_C_LONGINT,long)
            cast_helper(MT_C_DBL,     double,     MT_C_LONGINT,long)
            cast_helper(MT_C_LDBL,    long double,MT_C_LONGINT,long)
            cast_helper(MT_C_INT,     int,        MT_C_LONGLONG,long long)
            cast_helper(MT_C_LONGINT, long,       MT_C_LONGLONG,long long)
            cast_helper(MT_C_LONGLONG,long long,  MT_C_LONGLONG,long long)
            cast_helper(MT_C_FLOAT,   float,      MT_C_LONGLONG,long long)
            cast_helper(MT_C_DBL,     double,     MT_C_LONGLONG,long long)
            cast_helper(MT_C_LDBL,    long double,MT_C_LONGLONG,long long)
            cast_helper(MT_C_INT,     int,        MT_C_FLOAT,float)
            cast_helper(MT_C_LONGINT, long,       MT_C_FLOAT,float)
            cast_helper(MT_C_LONGLONG,long long,  MT_C_FLOAT,float)
            cast_helper(MT_C_FLOAT,   float,      MT_C_FLOAT,float)
            cast_helper(MT_C_DBL,     double,     MT_C_FLOAT,float)
            cast_helper(MT_C_LDBL,    long double,MT_C_FLOAT,float)
            cast_helper(MT_C_INT,     int,        MT_C_DBL,double)
            cast_helper(MT_C_LONGINT, long,       MT_C_DBL,double)
            cast_helper(MT_C_LONGLONG,long long,  MT_C_DBL,double)
            cast_helper(MT_C_FLOAT,   float,      MT_C_DBL,double)
            cast_helper(MT_C_DBL,     double,     MT_C_DBL,double)
            cast_helper(MT_C_LDBL,    long double,MT_C_DBL,double)
            cast_helper(MT_C_INT,     int,        MT_C_LDBL,long double)
            cast_helper(MT_C_LONGINT, long,       MT_C_LDBL,long double)
            cast_helper(MT_C_LONGLONG,long long,  MT_C_LDBL,long double)
            cast_helper(MT_C_FLOAT,   float,      MT_C_LDBL,long double)
            cast_helper(MT_C_DBL,     double,     MT_C_LDBL,long double)
            cast_helper(MT_C_LDBL,    long double,MT_C_LDBL,long double)
            {
                GA_Error("Types not recognized for cast",0);
            }
#undef cast_helper
            NGA_Release64(that.handle, &lo[0], &hi[0]);
            NGA_Release_update64(handle, &lo[0], &hi[0]);
        } else {
            // we don't own any of the data, do nothing
        }
    }

    return *this;
}


void Array::get_distribution()
{
    int64_t llo[GA_MAX_DIM], lhi[GA_MAX_DIM];
    NGA_Distribution64(handle, GA_Nodeid(), llo, lhi);
    lo.assign(llo, llo+shape.size());
    hi.assign(lhi, lhi+shape.size());
}


/**
 * Return true if this process owns a portion of this Array.
 */
bool Array::has_data() const
{
    return !(less_than(lo.begin(),lo.end(),0)
            || less_than(hi.begin(),hi.end(),0));
}


int64_t Array::get_count() const
{
    return accumulate(shape.begin(),shape.end(),1,multiplies<int64_t>());
}
