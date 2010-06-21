#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <typeinfo>

#include <ga.h>

#include "GlobalArray.H"
#include "NotImplementedException.H"
#include "Timing.H"


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


GlobalArray::GlobalArray(DataType type, vector<int64_t> shape)
    :   Array()
    ,   handle(0)
    ,   type(type)
    ,   shape(shape)
    ,   lo()
    ,   hi()
{
    handle = NGA_Create64(type, shape.size(), &shape[0], "noname", NULL);
    set_distribution();
}


GlobalArray::GlobalArray(DataType type, vector<Dimension*> dims)
    :   Array()
    ,   handle(0)
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
    set_distribution();
}


GlobalArray::GlobalArray(const GlobalArray &that)
    :   Array()
    ,   handle(0)
    ,   type(that.type)
    ,   shape(that.shape)
    ,   lo(that.lo)
    ,   hi(that.hi)
{
    handle = GA_Duplicate(that.handle, "noname");
}


GlobalArray::~GlobalArray()
{
    GA_Destroy(handle);
}


DataType GlobalArray::get_type() const
{
    return type;
}


vector<int64_t> GlobalArray::get_shape() const
{
    return shape;
}


vector<int64_t> GlobalArray::get_local_shape() const
{
    vector<int64_t> local_shape(shape.size());
    for (size_t i=0,limit=shape.size(); i<limit; ++i) {
        local_shape.push_back(hi[i]-lo[i]+1);
    }
    return local_shape;
}


int64_t GlobalArray::get_ndim() const
{
    return shape.size();
}


void GlobalArray::fill(void *value)
{
    GA_Fill(handle, value);
}


void GlobalArray::copy(const Array *src)
{
    if (typeid(*src) == typeid(*this)) {
        GA_Copy(((GlobalArray*)src)->handle, handle);
    }
    throw NotImplementedException("GlobalArray::copy(Array*)");
}


void GlobalArray::copy(const Array *src,
        const vector<int64_t> &src_lo,
        const vector<int64_t> &src_hi,
        const vector<int64_t> &dst_lo,
        const vector<int64_t> &dst_hi)
{
}


ostream& operator << (ostream &os, const GlobalArray &array)
{
    os << "GlobalArray(" << array.get_type() << ",";
    vector<int64_t> shape = array.get_shape();
    os << "shape(" << shape[0];
    for (size_t i=1,limit=shape.size(); i<limit; ++i) {
        os << "," << shape[i];
    }
    os << "))";
    return os;
}


GlobalArray& GlobalArray::operator=(const GlobalArray &that)
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
        if (owns_data()) {
            vector<int64_t> ld(lo.size());
            void *this_data;
            void *that_data;
            NGA_Access64(handle,      &lo[0], &hi[0], &this_data, &ld[0]);
            NGA_Access64(that.handle, &lo[0], &hi[0], &that_data, &ld[0]);
#define cast_helper(src_mt,src_t,dst_mt,dst_t) \
            if (type == dst_mt && that.type == src_mt) { \
                dst_t *dst = (dst_t*)this_data; \
                src_t *src = (src_t*)that_data; \
                cast(dst,dst+get_size(),src); \
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
    } else {
        GA_Error("shape mismatch",0);
    }

    return *this;
}


GlobalArray& GlobalArray::operator+=(const GlobalArray &that)
{
    if (type == that.type && shape == that.shape) {
#define operator_helper(mt,t) \
        if (type == mt) { \
            t alpha = 1, beta = 1; \
            GA_Add(&alpha, handle, &beta, that.handle, handle); \
        } else
        operator_helper(MT_C_INT,     int)
        operator_helper(MT_C_LONGINT, long)
        operator_helper(MT_C_LONGLONG,long long)
        operator_helper(MT_C_FLOAT,   float)
        operator_helper(MT_C_DBL,     double)
        operator_helper(MT_C_LDBL,    long double)
        ; // for last else above
#undef operator_helper
    } else if (shape == that.shape) {
        // we must cast to same type before operation
        GlobalArray casted(type, shape);
        casted = that;
#define operator_helper(mt,t) \
        if (type == mt) { \
            t alpha = 1, beta = 1; \
            GA_Add(&alpha, handle, &beta, casted.handle, handle); \
        } else
        operator_helper(MT_C_INT,     int)
        operator_helper(MT_C_LONGINT, long)
        operator_helper(MT_C_LONGLONG,long long)
        operator_helper(MT_C_FLOAT,   float)
        operator_helper(MT_C_DBL,     double)
        operator_helper(MT_C_LDBL,    long double)
        ; // for last else above
#undef operator_helper
    } else {
        GA_Error("shape mismatch",0);
    }
    return *this;
}


GlobalArray& GlobalArray::operator-=(const GlobalArray &that)
{
    if (type == that.type && shape == that.shape) {
#define operator_helper(mt,t) \
        if (type == mt) { \
            t alpha = 1, beta = -1; \
            GA_Add(&alpha, handle, &beta, that.handle, handle); \
        } else
        operator_helper(MT_C_INT,     int)
        operator_helper(MT_C_LONGINT, long)
        operator_helper(MT_C_LONGLONG,long long)
        operator_helper(MT_C_FLOAT,   float)
        operator_helper(MT_C_DBL,     double)
        operator_helper(MT_C_LDBL,    long double)
        ; // for last else above
#undef operator_helper
    } else if (shape == that.shape) {
        // we must cast to same type before operation
        GlobalArray casted(type, shape);
        casted = that;
#define operator_helper(mt,t) \
        if (type == mt) { \
            t alpha = 1, beta = -1; \
            GA_Add(&alpha, handle, &beta, casted.handle, handle); \
        } else
        operator_helper(MT_C_INT,     int)
        operator_helper(MT_C_LONGINT, long)
        operator_helper(MT_C_LONGLONG,long long)
        operator_helper(MT_C_FLOAT,   float)
        operator_helper(MT_C_DBL,     double)
        operator_helper(MT_C_LDBL,    long double)
        ; // for last else above
#undef operator_helper
    } else {
        GA_Error("shape mismatch",0);
    }
    return *this;
}


GlobalArray& GlobalArray::operator*=(const GlobalArray &that)
{
    if (type == that.type && shape == that.shape) {
#define operator_helper(mt,t) \
        if (type == mt) { \
            GA_Elem_multiply(handle, that.handle, handle); \
        } else
        operator_helper(MT_C_INT,     int)
        operator_helper(MT_C_LONGINT, long)
        operator_helper(MT_C_LONGLONG,long long)
        operator_helper(MT_C_FLOAT,   float)
        operator_helper(MT_C_DBL,     double)
        operator_helper(MT_C_LDBL,    long double)
        ; // for last else above
#undef operator_helper
    } else if (shape == that.shape) {
        // we must cast to same type before operation
        GlobalArray casted(type, shape);
        casted = that;
#define operator_helper(mt,t) \
        if (type == mt) { \
            GA_Elem_multiply(handle, casted.handle, handle); \
        } else
        operator_helper(MT_C_INT,     int)
        operator_helper(MT_C_LONGINT, long)
        operator_helper(MT_C_LONGLONG,long long)
        operator_helper(MT_C_FLOAT,   float)
        operator_helper(MT_C_DBL,     double)
        operator_helper(MT_C_LDBL,    long double)
        ; // for last else above
#undef operator_helper
    } else {
        GA_Error("shape mismatch",0);
    }
    return *this;
}


GlobalArray& GlobalArray::operator/=(const GlobalArray &that)
{
    if (type == that.type && shape == that.shape) {
#define operator_helper(mt,t) \
        if (type == mt) { \
            GA_Elem_divide(handle, that.handle, handle); \
        } else
        operator_helper(MT_C_INT,     int)
        operator_helper(MT_C_LONGINT, long)
        operator_helper(MT_C_LONGLONG,long long)
        operator_helper(MT_C_FLOAT,   float)
        operator_helper(MT_C_DBL,     double)
        operator_helper(MT_C_LDBL,    long double)
        ; // for last else above
#undef operator_helper
    } else if (shape == that.shape) {
        // we must cast to same type before operation
        GlobalArray casted(type, shape);
        casted = that;
#define operator_helper(mt,t) \
        if (type == mt) { \
            GA_Elem_divide(handle, casted.handle, handle); \
        } else
        operator_helper(MT_C_INT,     int)
        operator_helper(MT_C_LONGINT, long)
        operator_helper(MT_C_LONGLONG,long long)
        operator_helper(MT_C_FLOAT,   float)
        operator_helper(MT_C_DBL,     double)
        operator_helper(MT_C_LDBL,    long double)
        ; // for last else above
#undef operator_helper
    } else {
        GA_Error("shape mismatch",0);
    }
    return *this;
}


void GlobalArray::set_distribution()
{
    int64_t llo[GA_MAX_DIM], lhi[GA_MAX_DIM];
    NGA_Distribution64(handle, GA_Nodeid(), llo, lhi);
    lo.assign(llo, llo+shape.size());
    hi.assign(lhi, lhi+shape.size());
}


/**
 * Return true if this process owns a portion of this GlobalArray.
 */
bool GlobalArray::owns_data() const
{
    return !(less_than(lo.begin(),lo.end(),0)
            || less_than(hi.begin(),hi.end(),0));
}


void GlobalArray::get_distribution(
        vector<int64_t> &alo, vector<int64_t> &ahi) const
{
    alo.assign(lo.begin(),lo.end());
    ahi.assign(hi.begin(),hi.end());
}


void* GlobalArray::access()
{
    void *tmp;
    vector<int64_t> ld(lo.size());
    NGA_Access64(handle, &lo[0], &hi[0], &tmp, &ld[0]);
    return tmp;
}


void* GlobalArray::access() const
{
    void *tmp;
    vector<int64_t> ld(lo.size());
    vector<int64_t> lo_copy(lo.begin(),lo.end());
    vector<int64_t> hi_copy(hi.begin(),hi.end());
    NGA_Access64(handle, &lo_copy[0], &hi_copy[0], &tmp, &ld[0]);
    return tmp;
}


void GlobalArray::release() const
{
    vector<int64_t> lo_copy(lo.begin(),lo.end());
    vector<int64_t> hi_copy(hi.begin(),hi.end());
    NGA_Release64(handle, &lo_copy[0], &hi_copy[0]);
}


void GlobalArray::release_update()
{
    NGA_Release_update64(handle, &lo[0], &hi[0]);
}


void* GlobalArray::get(
        const vector<int64_t> &alo,
        const vector<int64_t> &ahi) const
{
    void *buffer;
    int64_t buffer_size = 1;
    vector<int64_t> alo_copy(alo.begin(),alo.end());
    vector<int64_t> ahi_copy(ahi.begin(),ahi.end());
    vector<int64_t> ld;

    TIMING("GlobalArray::get(vector<int64_t>,vector<int64_t>)");

    for (size_t i=0,limit=get_ndim(); i<limit; ++i) {
        ld.push_back(ahi[i]-alo[i]+1);
        buffer_size *= ld.back();
    }

#define get_helper(mt,t) \
    if (type == mt) { \
        buffer = new t[buffer_size]; \
    } else
    get_helper(MT_C_INT,     int)
    get_helper(MT_C_LONGINT, long)
    get_helper(MT_C_LONGLONG,long long)
    get_helper(MT_C_FLOAT,   float)
    get_helper(MT_C_DBL,     double)
    get_helper(MT_C_LDBL,    long double)
    ; // for last else above
#undef get_helper
    
    NGA_Get64(handle, &alo_copy[0], &ahi_copy[0], buffer, &ld[1]);
    
    return buffer;
}


void GlobalArray::put(void *buffer,
        const vector<int64_t> &lo,
        const vector<int64_t> & hi)
{
    throw NotImplementedException("GlobalArray::put(void*,vector<int64_t>,vector<int64_t>)");
}


void GlobalArray::scatter(void *buffer, const vector<int64_t> &subscripts)
{
    throw NotImplementedException("GlobalArray::scatter(void*,vector<int64_t>)");
}


void* GlobalArray::gather(const vector<int64_t> &subscripts) const
{
    throw NotImplementedException("GlobalArray::gather(vector<int64_t>)");
}
