#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <sstream>
#include <typeinfo>

#include <ga.h>

#include "DataType.H"
#include "Debug.H"
#include "Error.H"
#include "GlobalArray.H"
#include "NotImplementedException.H"
#include "Timing.H"
#include "Util.H"


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


void GlobalArray::create()
{
    handle = NGA_Create64(type.to_ga(), shape.size(), &shape[0], "name", NULL);
    set_distribution();
}


void GlobalArray::create_scalar()
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        scalar = (void*)(new T); \
    } else
#include "DataType.def"
}


GlobalArray::GlobalArray(DataType type, vector<int64_t> shape)
    :   Array()
    ,   handle(0)
    ,   type(type)
    ,   shape(shape)
    ,   lo()
    ,   hi()
    ,   scalar(NULL)
{
    if (is_scalar()) {
        create_scalar();
    } else {
        create();
    }
}


GlobalArray::GlobalArray(DataType type, vector<Dimension*> dims)
    :   Array()
    ,   handle(0)
    ,   type(type)
    ,   shape()
    ,   lo()
    ,   hi()
    ,   scalar(NULL)
{
    for (vector<Dimension*>::const_iterator it=dims.begin(), end=dims.end();
            it!=end; ++it) {
        shape.push_back((*it)->get_size());
    }
    if (is_scalar()) {
        create_scalar();
    } else {
        create();
    }
}


GlobalArray::GlobalArray(const GlobalArray &that)
    :   Array()
    ,   handle(0)
    ,   type(that.type)
    ,   shape(that.shape)
    ,   lo(that.lo)
    ,   hi(that.hi)
    ,   scalar(NULL)
{
    if (is_scalar()) {
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            scalar = (void*)(new T); \
            *((T*)scalar) = *((T*)(that.scalar)); \
        } else
#include "DataType.def"
    } else {
        handle = GA_Duplicate(that.handle, "noname");
    }
}


GlobalArray::~GlobalArray()
{
    if (is_scalar()) {
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            delete ((T*)scalar); \
        } else
#include "DataType.def"
    } else {
        GA_Destroy(handle);
    }
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
        local_shape[i] = hi[i]-lo[i]+1;
    }

    return local_shape;
}


int64_t GlobalArray::get_ndim() const
{
    return shape.size();
}


void GlobalArray::fill(void *value)
{
    if (is_scalar()) {
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            *((T*)scalar) = *((T*)value); \
        } else
#include "DataType.def"
    } else {
        GA_Fill(handle, value);
    }
}


void GlobalArray::copy(const Array *src)
{
    if (typeid(*src) == typeid(*this)) {
        if (is_scalar()) {
#define DATATYPE_EXPAND(DT,T) \
            if (DT == type) { \
                *((T*)scalar) = *((T*)(((GlobalArray*)src)->scalar)); \
            } else
#include "DataType.def"
        } else {
            GA_Copy(((GlobalArray*)src)->handle, handle);
        }
    }
    throw NotImplementedException("GlobalArray::copy(Array*) of differing Array implementations");
}


void GlobalArray::copy(const Array *src,
        const vector<int64_t> &src_lo,
        const vector<int64_t> &src_hi,
        const vector<int64_t> &dst_lo,
        const vector<int64_t> &dst_hi)
{
    if (typeid(*src) == typeid(*this)) {
        if (is_scalar()) {
            throw NotImplementedException("GlobalArray::copy(Array*,vector<int64_t>,vector<int64_t>,vector<int64_t>,vector<int64_t>) no implemented for scalars");
        } else {
            vector<int64_t> src_lo_copy(src_lo.begin(), src_lo.end());
            vector<int64_t> src_hi_copy(src_hi.begin(), src_hi.end());
            vector<int64_t> dst_lo_copy(dst_lo.begin(), dst_lo.end());
            vector<int64_t> dst_hi_copy(dst_hi.begin(), dst_hi.end());
            NGA_Copy_patch64('n',
                    ((GlobalArray*)src)->handle,
                    &src_lo_copy[0], &src_hi_copy[0],
                    handle,
                    &dst_lo_copy[0], &dst_hi_copy[0]);
        }
    }
    throw NotImplementedException("GlobalArray::copy(Array*,vector<int64_t>,vector<int64_t>,vector<int64_t>,vector<int64_t>) of differing Array implementations");
}


ostream& operator << (ostream &os, const GlobalArray &array)
{
    vector<int64_t> shape = array.get_shape();

    os << "GlobalArray(" << array.get_type() << ",shape(";
    if (!shape.empty()) {
        os << shape[0];
    }
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
                ERR("Types not recognized for cast");
            }
#undef cast_helper
            NGA_Release64(that.handle, &lo[0], &hi[0]);
            NGA_Release_update64(handle, &lo[0], &hi[0]);
        } else {
            // we don't own any of the data, do nothing
        }
    } else {
        ERR("shape mismatch");
    }

    return *this;
}


GlobalArray& GlobalArray::operator+=(const GlobalArray &that)
{
    if (type == that.type && shape == that.shape) {
#define GATYPE_EXPAND(mt,t) \
        if (type == mt) { \
            t alpha = 1, beta = 1; \
            GA_Add(&alpha, handle, &beta, that.handle, handle); \
        } else
#include "GlobalArray.def"
    } else if (shape == that.shape) {
        // we must cast to same type before operation
        GlobalArray casted(type, shape);
        casted = that;
#define GATYPE_EXPAND(mt,t) \
        if (type == mt) { \
            t alpha = 1, beta = 1; \
            GA_Add(&alpha, handle, &beta, casted.handle, handle); \
        } else
#include "GlobalArray.def"
    } else {
        ERR("shape mismatch");
    }

    return *this;
}


GlobalArray& GlobalArray::operator-=(const GlobalArray &that)
{
    if (type == that.type && shape == that.shape) {
#define GATYPE_EXPAND(mt,t) \
        if (type == mt) { \
            t alpha = 1, beta = -1; \
            GA_Add(&alpha, handle, &beta, that.handle, handle); \
        } else
#include "GlobalArray.def"
    } else if (shape == that.shape) {
        // we must cast to same type before operation
        GlobalArray casted(type, shape);
        casted = that;
#define GATYPE_EXPAND(mt,t) \
        if (type == mt) { \
            t alpha = 1, beta = -1; \
            GA_Add(&alpha, handle, &beta, casted.handle, handle); \
        } else
#include "GlobalArray.def"
    } else {
        ERR("shape mismatch");
    }

    return *this;
}


GlobalArray& GlobalArray::operator*=(const GlobalArray &that)
{
    if (type == that.type && shape == that.shape) {
#define GATYPE_EXPAND(mt,t) \
        if (type == mt) { \
            GA_Elem_multiply(handle, that.handle, handle); \
        } else
#include "GlobalArray.def"
    } else if (shape == that.shape) {
        // we must cast to same type before operation
        GlobalArray casted(type, shape);
        casted = that;
#define GATYPE_EXPAND(mt,t) \
        if (type == mt) { \
            GA_Elem_multiply(handle, casted.handle, handle); \
        } else
#include "GlobalArray.def"
    } else {
        ERR("shape mismatch");
    }

    return *this;
}


GlobalArray& GlobalArray::operator/=(const GlobalArray &that)
{
    if (type == that.type && shape == that.shape) {
#define GATYPE_EXPAND(mt,t) \
        if (type == mt) { \
            GA_Elem_divide(handle, that.handle, handle); \
        } else
#include "GlobalArray.def"
    } else if (shape == that.shape) {
        // we must cast to same type before operation
        GlobalArray casted(type, shape);
        casted = that;
#define GATYPE_EXPAND(mt,t) \
        if (type == mt) { \
            GA_Elem_divide(handle, casted.handle, handle); \
        } else
#include "GlobalArray.def"
    } else {
        ERR("shape mismatch");
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


bool GlobalArray::is_scalar() const
{
    return shape.empty();
}


/**
 * Return true if this process owns a portion of this GlobalArray.
 */
bool GlobalArray::owns_data() const
{
    if (is_scalar()) {
        return true;
    } else {
        return !(less_than(lo.begin(),lo.end(),0)
                || less_than(hi.begin(),hi.end(),0));
    }
}


void GlobalArray::get_distribution(
        vector<int64_t> &alo, vector<int64_t> &ahi) const
{
    alo.assign(lo.begin(),lo.end());
    ahi.assign(hi.begin(),hi.end());
}


void* GlobalArray::access()
{
    if (is_scalar()) {
        return scalar;
    } else {
        void *tmp;
        vector<int64_t> ld(lo.size());
        NGA_Access64(handle, &lo[0], &hi[0], &tmp, &ld[0]);
        return tmp;
    }
}


void* GlobalArray::access() const
{
    if (is_scalar()) {
        return scalar;
    } else {
        void *tmp;
        vector<int64_t> ld(lo.size());
        vector<int64_t> lo_copy(lo.begin(),lo.end());
        vector<int64_t> hi_copy(hi.begin(),hi.end());
        NGA_Access64(handle, &lo_copy[0], &hi_copy[0], &tmp, &ld[0]);
        return tmp;
    }
}


void GlobalArray::release() const
{
    if (is_scalar()) {
    } else {
        vector<int64_t> lo_copy(lo.begin(),lo.end());
        vector<int64_t> hi_copy(hi.begin(),hi.end());
        NGA_Release64(handle, &lo_copy[0], &hi_copy[0]);
    }
}


void GlobalArray::release_update()
{
    if (is_scalar()) {
    } else {
        NGA_Release_update64(handle, &lo[0], &hi[0]);
    }
}


void* GlobalArray::get(
        void *buffer,
        const vector<int64_t> &lo,
        const vector<int64_t> &hi,
        const vector<int64_t> &ld) const
{
    vector<int64_t> lo_copy(lo.begin(),lo.end());
    vector<int64_t> hi_copy(hi.begin(),hi.end());
    vector<int64_t> ld_copy(ld.begin(),ld.end());

    TIMING("GlobalArray::get(void*,vector<int64_t>,vector<int64_t>,vector<int64_t>)");

    NGA_Get64(handle, &lo_copy[0], &hi_copy[0], buffer, &ld_copy[0]);

    return buffer;
}


void GlobalArray::put(void *buffer,
        const vector<int64_t> &lo,
        const vector<int64_t> &hi,
        const vector<int64_t> &ld)
{
    vector<int64_t> lo_copy(lo.begin(), lo.end());
    vector<int64_t> hi_copy(hi.begin(), hi.end());
    vector<int64_t> ld_copy(ld.begin(), ld.end());
    NGA_Put64(handle, &lo_copy[0], &hi_copy[0], buffer, &ld_copy[0]);
}


void GlobalArray::scatter(void *buffer, vector<int64_t> &subscripts)
{
    // GA has a funky C signature for this one...
    int64_t ndim = get_ndim();
    int64_t n = subscripts.size() / ndim;
    int64_t **subs = new int64_t*[n];

    TIMING("GlobalArray::scatter(void*,vector<int64_t>)");

    for (size_t i=0; i<n; ++i) {
        subs[i] = &subscripts[i*ndim];
    }

    NGA_Scatter64(handle, buffer, subs, n);

    delete [] subs;
}


void* GlobalArray::gather(vector<int64_t> &subscripts) const
{
    // GA has a funky C signature for this one...
    int64_t ndim = get_ndim();
    int64_t n = subscripts.size() / ndim;
    int64_t **subs = new int64_t*[n];
    void *buffer;

    TIMING("GlobalArray::scatter(void*,vector<int64_t>)");

    for (size_t i=0; i<n; ++i) {
        subs[i] = &subscripts[i*ndim];
    }

#define GATYPE_EXPAND(mt,t) \
    if (type == mt) { \
        buffer = new t[n]; \
    } else
#include "GlobalArray.def"
    
    NGA_Gather64(handle, buffer, subs, n);

    delete subs;
}


ostream& GlobalArray::print(ostream &os) const
{
    os << "GlobalArray";
    return os;
}


void GlobalArray::dump() const
{
    if (is_scalar()) {
#define GATYPE_EXPAND(mt,t) \
        if (type == mt) { \
            std::ostringstream os; \
            os << *((t*)scalar) << std::endl; \
            pagoda::print_zero(os.str()); \
        } else
#include "GlobalArray.def"
    } else {
        GA_Print(handle);
    }
}
