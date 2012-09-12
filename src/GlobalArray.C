#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <algorithm>
#include <functional>
#include <sstream>
#include <typeinfo>

#include <ga.h>
#include <macdecls.h>

#include "Bootstrap.H"
#include "Copy.H"
#include "DataType.H"
#include "Debug.H"
#include "Error.H"
#include "GlobalArray.H"
#include "Print.H"
#include "ScalarArray.H"
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


int GlobalArray::to_ga(const DataType &type)
{

    if (false) {
        /*
        } else if (operator==(DataType::CHAR)) {
            return C_CHAR;
        */
    }
    else if (type==(DataType::SHORT)) {
        throw DataTypeException("GA does not support C short");
    }
    else if (type==(DataType::INT)) {
        return C_INT;
    }
    else if (type==(DataType::LONG)) {
        return C_LONG;
    }
    else if (type==(DataType::LONGLONG)) {
        return C_LONGLONG;
    }
    else if (type==(DataType::FLOAT)) {
        return C_FLOAT;
    }
    else if (type==(DataType::DOUBLE)) {
        return C_DBL;
    }
    else if (type==(DataType::LONGDOUBLE)) {
#ifdef C_LDBL
        return C_LDBL;
#else
        throw DataTypeException("GA does not support C long double");
#endif
    }
    else if (type==(DataType::UCHAR)) {
        throw DataTypeException("GA does not support C unsigned char");
    }
    else if (type==(DataType::USHORT)) {
        throw DataTypeException("GA does not support C unsigned short");
    }
    else if (type==(DataType::UINT)) {
        throw DataTypeException("GA does not support C unsigned int");
    }
    else if (type==(DataType::ULONG)) {
        throw DataTypeException("GA does not support C unsigned long");
    }
    else if (type==(DataType::ULONGLONG)) {
        throw DataTypeException("GA does not support C unsigned long long");
    }
    else if (type==(DataType::STRING)) {
        throw DataTypeException("GA does not support C char*");
    }

    throw DataTypeException("could not determine GA type from DataType");
}


DataType GlobalArray::to_dt(int type)
{

    if (C_INT == type) {
        return DataType::INT;
    }
    else if (C_LONG == type) {
        return DataType::LONG;
    }
    else if (C_LONGLONG == type) {
        return DataType::LONGLONG;
    }
    else if (C_FLOAT == type) {
        return DataType::FLOAT;
    }
    else if (C_DBL == type) {
        return DataType::DOUBLE;
#ifdef C_LDBL
    }
    else if (C_LDBL == type) {
        return DataType::LONGDOUBLE;
#endif
    }
    throw DataTypeException("could not determine DataType from int");
}


void GlobalArray::create()
{
#if 1
    /* mpich2 seems to handle distribution of the slowest (first) dimension
     * better than the fastest (last) dimension
     * for now, only distribute the first dimension, possibly the first two
     * since in some cases the first dimension may have a size of 1 */
    vector<int64_t> chunk = shape;
    if (pagoda::num_nodes() < chunk.front()) {
        chunk.front() = -1; /* only distribute first dimension */
    }
    else {
        if (chunk.size() > 2) {
            /* chunk only the first two dimensions */
            chunk[0] = -1;
            chunk[1] = -1;
        }
        else {
            /* remove the custom chunking ie leave it up to GA */
            chunk.assign(chunk.size(), -1);
        }
    }
    //handle = NGA_Create64(to_ga(type), shape.size(), &shape[0], "name",
    //        &chunk[0]);
    handle = NGA_Create_config64(to_ga(type), shape.size(), &shape[0], "name",
            &chunk[0], group.get_id());
#else
    //handle = NGA_Create64(to_ga(type), shape.size(), &shape[0], "name", NULL);
    handle = NGA_Create_config64(to_ga(type), shape.size(), &shape[0], "name",
            NULL, group.get_id());
#endif
    set_distribution();
}


GlobalArray::GlobalArray(DataType type, vector<int64_t> shape)
    :   AbstractArray(type)
    ,   handle(0)
    ,   type(type)
    ,   shape(shape)
    ,   lo(shape.size())
    ,   hi(shape.size())
    ,   group(ProcessGroup::get_default())
{
    create();
}


GlobalArray::GlobalArray(DataType type, vector<int64_t> shape,
        const ProcessGroup &group)
    :   AbstractArray(type)
    ,   handle(0)
    ,   type(type)
    ,   shape(shape)
    ,   lo(shape.size())
    ,   hi(shape.size())
    ,   group(group)
{
    create();
}


GlobalArray::GlobalArray(DataType type, vector<Dimension*> dims)
    :   AbstractArray(type)
    ,   handle(0)
    ,   type(type)
    ,   shape()
    ,   lo(dims.size())
    ,   hi(dims.size())
    ,   group(ProcessGroup::get_default())
{
    for (vector<Dimension*>::const_iterator it=dims.begin(), end=dims.end();
            it!=end; ++it) {
        shape.push_back((*it)->get_size());
    }
    create();
}


GlobalArray::GlobalArray(DataType type, vector<Dimension*> dims,
        const ProcessGroup &group)
    :   AbstractArray(type)
    ,   handle(0)
    ,   type(type)
    ,   shape()
    ,   lo(dims.size())
    ,   hi(dims.size())
    ,   group(group)
{
    for (vector<Dimension*>::const_iterator it=dims.begin(), end=dims.end();
            it!=end; ++it) {
        shape.push_back((*it)->get_size());
    }
    create();
}


GlobalArray::GlobalArray(const GlobalArray &that)
    :   AbstractArray(that)
    ,   handle(0)
    ,   type(that.type)
    ,   shape(that.shape)
    ,   lo(that.lo)
    ,   hi(that.hi)
    ,   group(that.group)
{
    handle = GA_Duplicate(that.handle, "noname");
    GA_Copy(that.handle,handle);
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
    GA_Fill(handle, value);
}


void GlobalArray::copy(const Array *src)
{
    const GlobalArray *ga_src = dynamic_cast<const GlobalArray*>(src);
    if (type != src->get_type()) {
        ERR("arrays must be same type");
    }
    if (shape != src->get_shape()) {
        ERR("arrays must be same shape");
    }
    if (ga_src && GA_Compare_distr(handle, ga_src->handle) == 0) {
        GA_Copy(((GlobalArray*)ga_src)->handle, handle);
    }
    else {
        // idea is that each process accesses local data and get()s the passed
        // in array's data into those buffers.
        if (owns_data()) {
            void *data = access();
            data = src->get(lo, hi, data);
            release_update();
        }
    }
}


void GlobalArray::copy(const Array *src,
                       const vector<int64_t> &src_lo,
                       const vector<int64_t> &src_hi,
                       const vector<int64_t> &dst_lo,
                       const vector<int64_t> &dst_hi)
{
    const GlobalArray *ga_src = dynamic_cast<const GlobalArray*>(src);
    if (type != src->get_type()) {
        ERR("arrays must be same type");
    }
    if (ga_src) {
        vector<int64_t> src_lo_copy(src_lo.begin(), src_lo.end());
        vector<int64_t> src_hi_copy(src_hi.begin(), src_hi.end());
        vector<int64_t> dst_lo_copy(dst_lo.begin(), dst_lo.end());
        vector<int64_t> dst_hi_copy(dst_hi.begin(), dst_hi.end());
        NGA_Copy_patch64('n',
                         ga_src->handle, &src_lo_copy[0], &src_hi_copy[0],
                         handle,         &dst_lo_copy[0], &dst_hi_copy[0]);
    }
    else {
        // dumbest implementation...
        if (0 == pagoda::me) {
            void *data = src->get(src_lo, src_hi);
            this->put(data, dst_lo, dst_hi);
        }
    }
}


GlobalArray* GlobalArray::clone() const
{
    return new GlobalArray(*this);
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


GlobalArray* GlobalArray::cast(DataType new_type) const
{
    GlobalArray *dst_array;

    if (type == new_type) {
        // distributions are assumed to be identical
        dst_array = new GlobalArray(*this);
    }
    else {
        // distributions are assumed to be identical
        // types are different, so this is a cast
        dst_array = new GlobalArray(new_type, shape);
        if (owns_data()) {
            vector<int64_t> clo = lo;
            vector<int64_t> chi = hi;
            vector<int64_t> ld(lo.size());
            void *src_data;
            void *dst_data;
            NGA_Access64(handle,           &clo[0], &chi[0], &src_data, &ld[0]);
            NGA_Access64(dst_array->handle,&clo[0], &chi[0], &dst_data, &ld[0]);
#define cast_helper(src_mt,src_t,dst_mt,dst_t) \
            if (to_ga(type) == src_mt && to_ga(dst_array->type) == dst_mt) { \
                src_t *src = (src_t*)src_data; \
                dst_t *dst = (dst_t*)dst_data; \
                pagoda::copy_cast<dst_t>(src,src+get_size(),dst); \
            } else
            cast_helper(C_INT,     int,        C_INT,int)
            cast_helper(C_LONG,    long,       C_INT,int)
            cast_helper(C_LONGLONG,long long,  C_INT,int)
            cast_helper(C_FLOAT,   float,      C_INT,int)
            cast_helper(C_DBL,     double,     C_INT,int)
#ifdef C_LDBL
            cast_helper(C_LDBL,    long double,C_INT,int)
#endif
            cast_helper(C_INT,     int,        C_LONG,long)
            cast_helper(C_LONG,    long,       C_LONG,long)
            cast_helper(C_LONGLONG,long long,  C_LONG,long)
            cast_helper(C_FLOAT,   float,      C_LONG,long)
            cast_helper(C_DBL,     double,     C_LONG,long)
#ifdef C_LDBL
            cast_helper(C_LDBL,    long double,C_LONG,long)
#endif
            cast_helper(C_INT,     int,        C_LONGLONG,long long)
            cast_helper(C_LONG,    long,       C_LONGLONG,long long)
            cast_helper(C_LONGLONG,long long,  C_LONGLONG,long long)
            cast_helper(C_FLOAT,   float,      C_LONGLONG,long long)
            cast_helper(C_DBL,     double,     C_LONGLONG,long long)
#ifdef C_LDBL
            cast_helper(C_LDBL,    long double,C_LONGLONG,long long)
#endif
            cast_helper(C_INT,     int,        C_FLOAT,float)
            cast_helper(C_LONG,    long,       C_FLOAT,float)
            cast_helper(C_LONGLONG,long long,  C_FLOAT,float)
            cast_helper(C_FLOAT,   float,      C_FLOAT,float)
            cast_helper(C_DBL,     double,     C_FLOAT,float)
#ifdef C_LDBL
            cast_helper(C_LDBL,    long double,C_FLOAT,float)
#endif
            cast_helper(C_INT,     int,        C_DBL,double)
            cast_helper(C_LONG,    long,       C_DBL,double)
            cast_helper(C_LONGLONG,long long,  C_DBL,double)
            cast_helper(C_FLOAT,   float,      C_DBL,double)
            cast_helper(C_DBL,     double,     C_DBL,double)
#ifdef C_LDBL
            cast_helper(C_LDBL,    long double,C_DBL,double)
            cast_helper(C_INT,     int,        C_LDBL,long double)
            cast_helper(C_LONG,    long,       C_LDBL,long double)
            cast_helper(C_LONGLONG,long long,  C_LDBL,long double)
            cast_helper(C_FLOAT,   float,      C_LDBL,long double)
            cast_helper(C_DBL,     double,     C_LDBL,long double)
            cast_helper(C_LDBL,    long double,C_LDBL,long double)
#endif
            {
                ERR("Types not recognized for cast");
            }
#undef cast_helper
            NGA_Release64(handle,                   &clo[0], &chi[0]);
            NGA_Release_update64(dst_array->handle, &clo[0], &chi[0]);
        }
        else {
            // we don't own any of the data, do nothing
        }
    }

    return dst_array;
}


GlobalArray& GlobalArray::operator=(const GlobalArray &that)
{
    if (this == &that) {
        return *this;
    }

    if (handle == 0) {
        GA_Destroy(handle);
    }

    handle = 0;
    type = that.type;
    shape = that.shape;
    lo = that.lo;
    hi = that.hi;

    handle = GA_Duplicate(that.handle, "noname");
    GA_Copy(that.handle,handle);

    return *this;
}


void GlobalArray::operate_add(int that_handle)
{
#define GATYPE_EXPAND(mt,t) \
    if (to_ga(type) == mt) { \
        t alpha = 1, beta = 1; \
        GA_Add(&alpha, handle, &beta, that_handle, handle); \
    } else
#include "GlobalArray.def"
}


void GlobalArray::operate_add_patch(int that_handle,
                                    vector<int64_t> &my_lo, vector<int64_t> &my_hi,
                                    vector<int64_t> &that_lo, vector<int64_t> &that_hi)
{
#define GATYPE_EXPAND(mt,t) \
    if (to_ga(type) == mt) { \
        t alpha = 1, beta = 1; \
        NGA_Add_patch64(&alpha, handle, &my_lo[0], &my_hi[0], \
                &beta, that_handle, &that_lo[0], &that_hi[0], \
                handle, &my_lo[0], &my_hi[0]); \
    } else
#include "GlobalArray.def"
}


GlobalArray GlobalArray::operator+(const GlobalArray &that) const
{
    GlobalArray ret(*this);
    ret += that;
    return ret;
}


GlobalArray GlobalArray::operator+(const ScalarArray &that) const
{
    GlobalArray ret(*this);
    ret += that;
    return ret;
}


GlobalArray& GlobalArray::operator+=(const GlobalArray &that)
{
    operate(that, &GlobalArray::operate_add, &GlobalArray::operate_add_patch);
    return *this;
}


GlobalArray& GlobalArray::operator+=(const ScalarArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        T cast = *((T*)that.get()); \
        GA_Add_constant(handle, &cast); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
    return *this;
}


void GlobalArray::operate_sub(int that_handle)
{
#define GATYPE_EXPAND(mt,t) \
    if (to_ga(type) == mt) { \
        t alpha = 1, beta = -1; \
        GA_Add(&alpha, handle, &beta, that_handle, handle); \
    } else
#include "GlobalArray.def"
}


void GlobalArray::operate_sub_patch(int that_handle,
                                    vector<int64_t> &my_lo, vector<int64_t> &my_hi,
                                    vector<int64_t> &that_lo, vector<int64_t> &that_hi)
{
#define GATYPE_EXPAND(mt,t) \
    if (to_ga(type) == mt) { \
        t alpha = 1, beta = -1; \
        NGA_Add_patch64(&alpha, handle, &my_lo[0], &my_hi[0], \
                &beta, that_handle, &that_lo[0], &that_hi[0], \
                handle, &my_lo[0], &my_hi[0]); \
    } else
#include "GlobalArray.def"
}


GlobalArray GlobalArray::operator-(const GlobalArray &that) const
{
    GlobalArray ret(*this);
    ret -= that;
    return ret;
}


GlobalArray GlobalArray::operator-(const ScalarArray &that) const
{
    GlobalArray ret(*this);
    ret -= that;
    return ret;
}


GlobalArray& GlobalArray::operator-=(const GlobalArray &that)
{
    operate(that, &GlobalArray::operate_sub, &GlobalArray::operate_sub_patch);
    return *this;
}


GlobalArray& GlobalArray::operator-=(const ScalarArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        T cast = -(*((T*)that.get())); \
        GA_Add_constant(handle, &cast); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
    return *this;
}


void GlobalArray::operate_mul(int that_handle)
{
    GA_Elem_multiply(handle, that_handle, handle);
}


void GlobalArray::operate_mul_patch(int that_handle,
                                    vector<int64_t> &my_lo, vector<int64_t> &my_hi,
                                    vector<int64_t> &that_lo, vector<int64_t> &that_hi)
{
    GA_Elem_multiply_patch64(
        handle, &my_lo[0], &my_hi[0],
        that_handle, &that_lo[0], &that_hi[0],
        handle, &my_lo[0], &my_hi[0]);
}


GlobalArray GlobalArray::operator*(const GlobalArray &that) const
{
    GlobalArray ret(*this);
    ret *= that;
    return ret;
}


GlobalArray GlobalArray::operator*(const ScalarArray &that) const
{
    GlobalArray ret(*this);
    ret *= that;
    return ret;
}


GlobalArray& GlobalArray::operator*=(const GlobalArray &that)
{
    operate(that, &GlobalArray::operate_mul, &GlobalArray::operate_mul_patch);
    return *this;
}


GlobalArray& GlobalArray::operator*=(const ScalarArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        T cast = *((T*)that.get()); \
        GA_Scale(handle, &cast); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
    return *this;
}


void GlobalArray::operate_div(int that_handle)
{
    GA_Elem_divide(handle, that_handle, handle);
}


void GlobalArray::operate_div_patch(int that_handle,
                                    vector<int64_t> &my_lo, vector<int64_t> &my_hi,
                                    vector<int64_t> &that_lo, vector<int64_t> &that_hi)
{
    GA_Elem_divide_patch64(
        handle, &my_lo[0], &my_hi[0],
        that_handle, &that_lo[0], &that_hi[0],
        handle, &my_lo[0], &my_hi[0]);
}


GlobalArray GlobalArray::operator/(const GlobalArray &that) const
{
    GlobalArray ret(*this);
    ret /= that;
    return ret;
}


GlobalArray GlobalArray::operator/(const ScalarArray &that) const
{
    GlobalArray ret(*this);
    ret /= that;
    return ret;
}


GlobalArray& GlobalArray::operator/=(const GlobalArray &that)
{
    operate(that, &GlobalArray::operate_div, &GlobalArray::operate_div_patch);
    return *this;
}


GlobalArray& GlobalArray::operator/=(const ScalarArray &that)
{
    /** @todo GA_Scale won't work for integers! */
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        T cast = static_cast<T>(1.0 / *((T*)that.get())); \
        GA_Scale(handle, &cast); \
    } else
#include "DataType.def"
    {
        EXCEPT(DataTypeException, "DataType not handled", type);
    }
    return *this;
}


void GlobalArray::operate(const GlobalArray &that,
                          GA_op_whole op_whole, GA_op_patch op_patch)
{
    const GlobalArray *casted;

    if (type != that.type) {
        casted = that.cast(type);
    }
    else {
        casted = &that;
    }

    if (shape == casted->get_shape()) {
        (this->*op_whole)(casted->handle);
    }
    else if (broadcast_check(casted)) {
        // shape mismatch, but broadcastable
        vector<int64_t> my_shape = shape;
        vector<int64_t> that_shape = casted->get_shape();
        vector<int64_t> broadcast_shape(my_shape.begin(),
                                        my_shape.begin()+(my_shape.size()-that_shape.size()));
        int64_t size = pagoda::shape_to_size(broadcast_shape);
        vector<int64_t> my_lo(my_shape.size(), 0);
        vector<int64_t> my_hi(my_shape);
        vector<int64_t> that_lo(that_shape.size(), 0);
        vector<int64_t> that_hi = that_shape;

        for (size_t i=0; i<my_hi.size(); ++i) {
            my_hi[i] -= 1;
        }
        for (size_t i=0; i<that_hi.size(); ++i) {
            that_hi[i] -= 1;
        }
        for (int64_t i=0; i<size; ++i) {
            vector<int64_t> front = pagoda::unravel_index(i,broadcast_shape);
            std::copy(front.begin(), front.end(), my_lo.begin());
            std::copy(front.begin(), front.end(), my_hi.begin());
            DEBUG_SYNC("  my_lo=" + pagoda::vec_to_string(my_lo) + "\n");
            DEBUG_SYNC("  my_hi=" + pagoda::vec_to_string(my_hi) + "\n");
            DEBUG_SYNC("that_lo=" + pagoda::vec_to_string(that_lo) + "\n");
            DEBUG_SYNC("that_hi=" + pagoda::vec_to_string(that_hi) + "\n");
            (this->*op_patch)(casted->handle, my_lo, my_hi, that_lo, that_hi);
        }
    }
    else {
        ERR("shape mismatch");
    }

    if (casted != &that) {
        delete casted;
    }
}


void GlobalArray::set_distribution()
{
    NGA_Distribution64(handle, GA_Nodeid(), &lo[0], &hi[0]);
    /** @todo verify lo and hi are valid, clear them if not so that we follow
     * the conventions in the documentation of Array::get_distribution */
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
    if (owns_data()) {
        void *tmp;
        vector<int64_t> ld(lo.size());
        NGA_Access64(handle, &lo[0], &hi[0], &tmp, &ld[0]);
        return tmp;
    }

    return NULL;
}


const void* GlobalArray::access() const
{
    if (owns_data()) {
        void *tmp;
        vector<int64_t> ld(lo.size());
        vector<int64_t> lo_copy(lo.begin(),lo.end());
        vector<int64_t> hi_copy(hi.begin(),hi.end());
        NGA_Access64(handle, &lo_copy[0], &hi_copy[0], &tmp, &ld[0]);
        return tmp;
    }

    return NULL;
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


void* GlobalArray::get(void *buffer) const
{
    return AbstractArray::get(buffer);
}


void* GlobalArray::get(int64_t lo, int64_t hi, void *buffer) const
{
    return AbstractArray::get(lo,hi,buffer);
}


void* GlobalArray::get(const vector<int64_t> &lo,const vector<int64_t> &hi,
                       void *buffer) const
{
    vector<int64_t> lo_copy(lo.begin(),lo.end());
    vector<int64_t> hi_copy(hi.begin(),hi.end());
    vector<int64_t> shape = pagoda::get_shape(lo,hi);


    if (buffer == NULL) {
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            buffer = static_cast<void*>(new T[pagoda::shape_to_size(shape)]); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    }

    NGA_Get64(handle, &lo_copy[0], &hi_copy[0], buffer, &shape[1]);

    return buffer;
}


void GlobalArray::put(void *buffer)
{
    AbstractArray::put(buffer);
}


void GlobalArray::put(void *buffer, int64_t lo, int64_t hi)
{
    AbstractArray::put(buffer,lo,hi);
}


void GlobalArray::put(void *buffer,
                      const vector<int64_t> &lo, const vector<int64_t> &hi)
{
    vector<int64_t> lo_copy(lo.begin(), lo.end());
    vector<int64_t> hi_copy(hi.begin(), hi.end());
    vector<int64_t> shape = pagoda::get_shape(lo, hi);
    NGA_Put64(handle, &lo_copy[0], &hi_copy[0], buffer, &shape[1]);
}


void GlobalArray::acc(void *buffer, void *alpha)
{
    AbstractArray::acc(buffer,alpha);
}


void GlobalArray::acc(void *buffer, int64_t lo, int64_t hi, void *alpha)
{
    AbstractArray::acc(buffer,lo,hi,alpha);
}


void GlobalArray::acc(void *buffer,
                      const vector<int64_t> &lo, const vector<int64_t> &hi,
                      void *alpha)
{
    vector<int64_t> lo_copy(lo.begin(), lo.end());
    vector<int64_t> hi_copy(hi.begin(), hi.end());
    vector<int64_t> shape = pagoda::get_shape(lo, hi);
    //NGA_Acc64(handle, &lo_copy[0], &hi_copy[0], buffer, &shape[1], alpha);
#define GATYPE_EXPAND(mt,t) \
    if (to_ga(type) == mt) { \
        t _alpha = 1; \
        if (alpha != NULL) { \
            _alpha = *static_cast<t*>(alpha); \
        } \
        NGA_Acc64(handle, &lo_copy[0], &hi_copy[0], buffer, &shape[1], &_alpha); \
    } else
#include "GlobalArray.def"
}


void GlobalArray::scatter(void *buffer, vector<int64_t> &subscripts)
{
    // GA has a funky C signature for this one...
    int64_t ndim = get_ndim();
    int64_t n = subscripts.size() / ndim;
    vector<int64_t*> subs(n,0);


    for (int64_t i=0; i<n; ++i) {
        subs[i] = &subscripts[i*ndim];
    }

    NGA_Scatter64(handle, buffer, &subs[0], n);
}


void* GlobalArray::gather(vector<int64_t> &subscripts, void *buffer) const
{
    // GA has a funky C signature for this one...
    int64_t ndim = get_ndim();
    int64_t n = subscripts.size() / ndim;
    vector<int64_t*> subs(n,0);


    for (int64_t i=0; i<n; ++i) {
        subs[i] = &subscripts[i*ndim];
    }

    if (buffer == NULL) {
#define GATYPE_EXPAND(mt,t) \
        if (to_ga(type) == mt) { \
            buffer = new t[n]; \
        } else
#include "GlobalArray.def"
    }

    NGA_Gather64(handle, buffer, &subs[0], n);

    return buffer;
}


#if 0
Array* GlobalArray::add(const Array *rhs) const
{
    const GlobalArray *array;
    const ScalarArray *scalar;
    GlobalArray *self_copy;

    if ((array = dynamic_cast<const GlobalArray*>(rhs))) {
        self_copy = new GlobalArray(*this);
        (*self_copy) += *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        self_copy = new GlobalArray(*this);
        (*self_copy) += *scalar;
    }
    else {
        ERR("GlobalArray::add(Array*) fell through");
    }

    return self_copy;
}


Array* GlobalArray::iadd(const Array *rhs)
{
    const GlobalArray *array;
    const ScalarArray *scalar;

    if ((array = dynamic_cast<const GlobalArray*>(rhs))) {
        (*this) += *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        (*this) += *scalar;
    }
    else {
        ERR("GlobalArray::iadd(Array*) fell through");
    }

    return this;
}


Array* GlobalArray::sub(const Array *rhs) const
{
    const GlobalArray *array;
    const ScalarArray *scalar;
    GlobalArray *self_copy;

    if ((array = dynamic_cast<const GlobalArray*>(rhs))) {
        self_copy = new GlobalArray(*this);
        (*self_copy) -= *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        self_copy = new GlobalArray(*this);
        (*self_copy) -= *scalar;
    }
    else {
        ERR("GlobalArray::sub(Array*) fell through");
    }

    return self_copy;
}


Array* GlobalArray::isub(const Array *rhs)
{
    const GlobalArray *array;
    const ScalarArray *scalar;

    if ((array = dynamic_cast<const GlobalArray*>(rhs))) {
        (*this) -= *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        (*this) -= *scalar;
    }
    else {
        ERR("GlobalArray::isub(Array*) fell through");
    }

    return this;
}


Array* GlobalArray::mul(const Array *rhs) const
{
    const GlobalArray *array;
    const ScalarArray *scalar;
    GlobalArray *self_copy;

    if ((array = dynamic_cast<const GlobalArray*>(rhs))) {
        self_copy = new GlobalArray(*this);
        (*self_copy) *= *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        self_copy = new GlobalArray(*this);
        (*self_copy) *= *scalar;
    }
    else {
        ERR("GlobalArray::mul(Array*) fell through");
    }

    return self_copy;
}


Array* GlobalArray::imul(const Array *rhs)
{
    const GlobalArray *array;
    const ScalarArray *scalar;

    if ((array = dynamic_cast<const GlobalArray*>(rhs))) {
        (*this) *= *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        (*this) *= *scalar;
    }
    else {
        ERR("GlobalArray::imul(Array*) fell through");
    }

    return this;
}


Array* GlobalArray::div(const Array *rhs) const
{
    const GlobalArray *array;
    const ScalarArray *scalar;
    GlobalArray *self_copy;

    if ((array = dynamic_cast<const GlobalArray*>(rhs))) {
        self_copy = new GlobalArray(*this);
        (*self_copy) /= *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        self_copy = new GlobalArray(*this);
        (*self_copy) /= *scalar;
    }
    else {
        ERR("GlobalArray::div(Array*) fell through");
    }

    return self_copy;
}


Array* GlobalArray::idiv(const Array *rhs)
{
    const GlobalArray *array;
    const ScalarArray *scalar;

    if ((array = dynamic_cast<const GlobalArray*>(rhs))) {
        (*this) /= *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        (*this) /= *scalar;
    }
    else {
        ERR("GlobalArray::idiv(Array*) fell through");
    }

    return this;
}
#endif


void GlobalArray::operate_max(int that_handle)
{
    GA_Elem_maximum(handle, that_handle, handle);
}


void GlobalArray::operate_max_patch(int that_handle,
                                    vector<int64_t> &my_lo, vector<int64_t> &my_hi,
                                    vector<int64_t> &that_lo, vector<int64_t> &that_hi)
{
    GA_Elem_maximum_patch64(
        handle, &my_lo[0], &my_hi[0],
        that_handle, &that_lo[0], &that_hi[0],
        handle, &my_lo[0], &my_hi[0]);
}


#if 0
Array* GlobalArray::max(const Array *rhs) const
{
    GlobalArray *self_copy = new GlobalArray(*this);
    self_copy->imax(rhs);
    return self_copy;
}


Array* GlobalArray::imax(const Array *rhs)
{
    const GlobalArray *array;
    const ScalarArray *scalar;

    if ((array = dynamic_cast<const GlobalArray*>(rhs))) {
        operate(*array,
                &GlobalArray::operate_max,
                &GlobalArray::operate_max_patch);
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            GlobalArray array_tmp(DT, shape); \
            array_tmp.fill_value(scalar->as<T>()); \
            operate(array_tmp, \
                    &GlobalArray::operate_max, \
                    &GlobalArray::operate_max_patch); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    }
    else {
        ERR("GlobalArray::imax(Array*) fell through");
    }

    return this;
}
#endif


void GlobalArray::operate_min(int that_handle)
{
    GA_Elem_minimum(handle, that_handle, handle);
}


void GlobalArray::operate_min_patch(int that_handle,
                                    vector<int64_t> &my_lo, vector<int64_t> &my_hi,
                                    vector<int64_t> &that_lo, vector<int64_t> &that_hi)
{
    GA_Elem_minimum_patch64(
        handle, &my_lo[0], &my_hi[0],
        that_handle, &that_lo[0], &that_hi[0],
        handle, &my_lo[0], &my_hi[0]);
}


#if 0
Array* GlobalArray::min(const Array *rhs) const
{
    GlobalArray *self_copy = new GlobalArray(*this);
    self_copy->imin(rhs);
    return self_copy;
}


Array* GlobalArray::imin(const Array *rhs)
{
    const GlobalArray *array;
    const ScalarArray *scalar;

    if ((array = dynamic_cast<const GlobalArray*>(rhs))) {
        operate(*array,
                &GlobalArray::operate_min,
                &GlobalArray::operate_min_patch);
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            GlobalArray array_tmp(DT, shape); \
            array_tmp.fill_value(scalar->as<T>()); \
            operate(array_tmp, \
                    &GlobalArray::operate_min, \
                    &GlobalArray::operate_min_patch); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    }
    else {
        ERR("GlobalArray::imin(Array*) fell through");
    }

    return this;
}


Array* GlobalArray::pow(double exponent) const
{
    GlobalArray *self_copy = new GlobalArray(*this);
    self_copy->ipow(exponent);
    return self_copy;
}


Array* GlobalArray::ipow(double exponent)
{
    if (owns_data()) {
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            T *data = (T*)access(); \
            for (int64_t i=0,limit=get_local_size(); i<limit; i++) { \
                data[i] = static_cast<T>(std::pow(static_cast<double>(data[i]),exponent)); \
            } \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
        release_update();
    }

    return this;
}
#endif


ostream& GlobalArray::print(ostream &os) const
{
    os << "GlobalArray";
    return os;
}


void GlobalArray::dump() const
{
    GA_Print(handle);
}
