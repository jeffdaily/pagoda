#if HAVE_CONFIG_H
#   include "config.h"
#endif

#include <stdint.h>

#include <cmath>

#include <ga.h>
#include <macdecls.h>

#include "Error.H"
#include "Print.H"
#include "GlobalArray.H"
#include "GlobalScalar.H"


GlobalScalar::GlobalScalar(DataType type)
    :   AbstractArray(type)
    ,   type(type)
    ,   handle(0)
    ,   owns(false)
    ,   group(ProcessGroup::get_default())
{
    int64_t ONE = 1;
    int64_t lo;
    int64_t hi;

    handle = NGA_Create_config64(GlobalArray::to_ga(type),
            1, &ONE, "name", NULL, group.get_id());
    NGA_Distribution64(handle, GA_Nodeid(), &lo, &hi);
    owns = (lo == 0 && hi == 0);
}


GlobalScalar::GlobalScalar(DataType type, ProcessGroup group)
    :   AbstractArray(type)
    ,   type(type)
    ,   handle(0)
    ,   owns(false)
    ,   group(group)
{
    int64_t ONE = 1;
    int64_t lo;
    int64_t hi;

    handle = NGA_Create_config64(GlobalArray::to_ga(type),
            1, &ONE, "name", NULL, group.get_id());
    NGA_Distribution64(handle, GA_Nodeid(), &lo, &hi);
    owns = (lo == 0 && hi == 0);
}


GlobalScalar::GlobalScalar(const GlobalScalar &that)
    :   AbstractArray(that)
    ,   type(that.type)
    ,   handle(0)
    ,   owns(false)
    ,   group(ProcessGroup::get_default())
{
    handle = GA_Duplicate(that.handle, "noname");
    GA_Copy(that.handle, handle);
}


GlobalScalar::~GlobalScalar()
{
    GA_Destroy(handle);
}


DataType GlobalScalar::get_type() const
{
    return type;
}


vector<int64_t> GlobalScalar::get_shape() const
{
    return vector<int64_t>();
}


vector<int64_t> GlobalScalar::get_local_shape() const
{
    if (owns) {
        return vector<int64_t>(1, 1);
    } else {
        return vector<int64_t>();
    }
}


int64_t GlobalScalar::get_ndim() const
{
    return 0;
}


void GlobalScalar::fill(void *value)
{
    GA_Fill(handle, value);
}


void GlobalScalar::copy(const Array *src)
{
    const GlobalScalar *gs_src = dynamic_cast<const GlobalScalar*>(src);
    if (type != src->get_type()) {
        ERR("arrays must be same type");
    }
    if (get_shape() != src->get_shape()) {
        ERR("arrays must be same shape");
    }
    if (gs_src) {
        GA_Copy(gs_src->handle, handle);
    }
    else {
        ERR("not implemented GlobalScalar::copy of mixed subclasses");
    }
}


void GlobalScalar::copy(const Array *src, const vector<int64_t> &src_lo, const vector<int64_t> &src_hi, const vector<int64_t> &dst_lo, const vector<int64_t> &dst_hi)
{
    ERR("not implemented GlobalScalar::copy patch");
}


GlobalScalar* GlobalScalar::clone() const
{
    return new GlobalScalar(*this);
}


bool GlobalScalar::owns_data() const
{
    return owns;
}


void GlobalScalar::get_distribution(vector<int64_t> &lo, vector<int64_t> &hi) const
{
    if (owns) {
        lo.assign(1, 0);
        hi.assign(1, 0);
    } else {
        lo.clear();
        hi.clear();
    }
}


void* GlobalScalar::access()
{
    if (owns) {
        void *tmp;
        int64_t lo = 0;
        int64_t hi = 0;
        NGA_Access64(handle, &lo, &hi, &tmp, NULL);
        return tmp;
    }

    return NULL;
}


const void* GlobalScalar::access() const
{
    if (owns) {
        void *tmp;
        int64_t lo = 0;
        int64_t hi = 0;
        NGA_Access64(handle, &lo, &hi, &tmp, NULL);
        return tmp;
    }

    return NULL;
}


void GlobalScalar::release() const
{
    if (owns) {
        int64_t lo = 0;
        int64_t hi = 0;
        NGA_Release64(handle, &lo, &hi);
    }
}


void GlobalScalar::release_update()
{
    if (owns) {
        int64_t lo = 0;
        int64_t hi = 0;
        NGA_Release_update64(handle, &lo, &hi);
    }
}


void* GlobalScalar::get(void *buffer) const
{
    return get(0, 0, buffer);
}


void* GlobalScalar::get(int64_t lo, int64_t hi, void *buffer) const
{
    return get(vector<int64_t>(1,lo), vector<int64_t>(1,hi), buffer);
}


void* GlobalScalar::get(const vector<int64_t> &lo, const vector<int64_t> &hi,
                       void *buffer) const
{
    int64_t _lo = 0;
    int64_t _hi = 0;

    ASSERT(lo.size() == 1);
    ASSERT(hi.size() == 1);
    ASSERT(lo.at(0) == 0);
    ASSERT(hi.at(0) == 0);

    if (buffer == NULL) {
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            buffer = static_cast<void*>(new T[1]); \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
    }

    NGA_Get64(handle, &_lo, &_hi, buffer, NULL);

    return buffer;
}


void GlobalScalar::put(void *buffer)
{
    put(buffer, 0, 0);
}


void GlobalScalar::put(void *buffer, int64_t lo, int64_t hi)
{
    put(buffer, vector<int64_t>(1,lo), vector<int64_t>(1,hi));
}


void GlobalScalar::put(void *buffer,
                      const vector<int64_t> &lo, const vector<int64_t> &hi)
{
    int64_t _lo = 0;
    int64_t _hi = 0;

    ASSERT(lo.size() == 1);
    ASSERT(hi.size() == 1);
    ASSERT(lo.at(0) == 0);
    ASSERT(hi.at(0) == 0);

    NGA_Put64(handle, &_lo, &_hi, buffer, NULL);
}


void GlobalScalar::acc(void *buffer, void *alpha)
{
    acc(buffer, 0, 0, alpha);
}


void GlobalScalar::acc(void *buffer, int64_t lo, int64_t hi, void *alpha)
{
    acc(buffer, vector<int64_t>(1,lo), vector<int64_t>(1,hi), alpha);
}


void GlobalScalar::acc(void *buffer,
                      const vector<int64_t> &lo, const vector<int64_t> &hi,
                      void *alpha)
{
    int64_t _lo = 0;
    int64_t _hi = 0;

    ASSERT(lo.size() == 1);
    ASSERT(hi.size() == 1);
    ASSERT(lo.at(0) == 0);
    ASSERT(hi.at(0) == 0);

#define GATYPE_EXPAND(mt,t) \
    if (GlobalArray::to_ga(type) == mt) { \
        t _alpha = 1; \
        if (alpha != NULL) { \
            _alpha = *static_cast<t*>(alpha); \
        } \
        NGA_Acc64(handle, &_lo, &_hi, buffer, NULL, &_alpha); \
    } else
#include "GlobalArray.def"
}


void GlobalScalar::scatter(void *buffer, vector<int64_t> &subscripts)
{
    put(buffer);
}


void* GlobalScalar::gather(vector<int64_t> &subscripts, void *buffer) const
{
    return get(buffer);
}


ostream& GlobalScalar::print(ostream &os) const
{
    os << "GlobalScalar";
    return os;
}


void GlobalScalar::dump() const
{
    GA_Print(handle);
}
