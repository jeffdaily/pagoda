#ifndef NODEZEROARRAY_H_
#define NODEZEROARRAY_H_

#include <cmath>
#include <vector>

#include "Array.H"
#include "DataType.H"
#include "Debug.H"
#include "Dimension.H"

using std::vector;


/**
 * An N-dimensional distributed Array with all data on node zero.
 */
template <class T>
class NodeZeroArray : public Array
{
    public:
        NodeZeroArray(DataType type);
        NodeZeroArray(const NodeZeroArray &that);
        virtual ~NodeZeroArray();

        virtual DataType get_type() const;
        virtual vector<int64_t> get_shape() const;
        virtual vector<int64_t> get_local_shape() const;
        virtual int64_t get_ndim() const;

        virtual void fill(void *value);

        virtual void copy(const Array *src);
        virtual void copy(const Array *src,
                const vector<int64_t> &src_lo,
                const vector<int64_t> &src_hi,
                const vector<int64_t> &dst_lo,
                const vector<int64_t> &dst_hi);

        virtual bool owns_data() const;
        virtual void get_distribution(
                vector<int64_t> &lo, vector<int64_t> &hi) const;
        virtual void* access();
        virtual void* access() const;
        virtual void release() const;
        virtual void release_update();

        virtual void* get() const;
        virtual void* get(void *buffer,
                          const vector<int64_t> &lo,
                          const vector<int64_t> &hi,
                          const vector<int64_t> &ld) const;
        virtual void put(void *buffer,
                         const vector<int64_t> &lo,
                         const vector<int64_t> &hi,
                         const vector<int64_t> &ld);
        virtual void scatter(void *buffer, vector<int64_t> &subscripts);
        virtual void* gather(vector<int64_t> &subscripts) const;

        virtual Array* add(const Array *rhs) const;
        virtual Array* sub(const Array *rhs) const;
        virtual Array* mul(const Array *rhs) const;
        virtual Array* div(const Array *rhs) const;
        virtual Array* max(const Array *rhs) const;
        virtual Array* min(const Array *rhs) const;
        virtual Array* pow(double exponent) const;
        virtual Array* iadd(const Array *rhs);
        virtual Array* isub(const Array *rhs);
        virtual Array* imul(const Array *rhs);
        virtual Array* idiv(const Array *rhs);
        virtual Array* imax(const Array *rhs);
        virtual Array* imin(const Array *rhs);
        virtual Array* ipow(double exponent);
        NodeZeroArray& operator+=(const NodeZeroArray &that);
        NodeZeroArray& operator-=(const NodeZeroArray &that);
        NodeZeroArray& operator*=(const NodeZeroArray &that);
        NodeZeroArray& operator/=(const NodeZeroArray &that);

        virtual ostream& print(ostream &os) const;

        virtual void dump() const;

        template <class cast_type> cast_type as() const {
#define DATATYPE_EXPAND(DT,T) \
            if (DT == type) { \
                T the_value = *((T*)(value)); \
                return static_cast<cast_type>(the_value); \
            } else
#include "DataType.def"
        }

    protected:
        DataType type;
        T *data;
};


template <class T>
NodeZeroArray::NodeZeroArray(DataType type)
    :   type(type)
    ,   value(NULL)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        value = (void*)(new T); \
    } else
#include "DataType.def"
}


template <class T>
NodeZeroArray::NodeZeroArray(const NodeZeroArray &that)
    :   type(that.type)
    ,   value(NULL)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        value = (void*)(new T); \
        *((T*)value) = *((T*)(that.value)); \
    } else
#include "DataType.def"
}


template <class T>
NodeZeroArray::~NodeZeroArray()
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        delete ((T*)value); \
    } else
#include "DataType.def"
}


template <class T>
DataType NodeZeroArray::get_type() const
{
    return type;
}


template <class T>
vector<int64_t> NodeZeroArray::get_shape() const
{
    return vector<int64_t>();
}


template <class T>
vector<int64_t> NodeZeroArray::get_local_shape() const
{
    return vector<int64_t>();
}


template <class T>
int64_t NodeZeroArray::get_ndim() const
{
    return 0;
}


template <class T>
void NodeZeroArray::fill(void *new_value)
{
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        *((T*)value) = *((T*)new_value); \
    } else
#include "DataType.def"
}


template <class T>
void NodeZeroArray::copy(const Array *src)
{
    const NodeZeroArray *sa_src = dynamic_cast<const NodeZeroArray*>(src);
    if (type != src->get_type()) {
        ERR("arrays must be same type");
    }
    if (get_shape() != src->get_shape()) {
        ERR("arrays must be same shape");
    }
    if (sa_src) {
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            *((T*)value) = *((T*)(sa_src->value)); \
        } else
#include "DataType.def"
    }
}


template <class T>
void NodeZeroArray::copy(const Array *src, const vector<int64_t> &src_lo, const vector<int64_t> &src_hi, const vector<int64_t> &dst_lo, const vector<int64_t> &dst_hi)
{
    ERR("not implemented NodeZeroArray::copy");
}


template <class T>
bool NodeZeroArray::owns_data() const
{
    if (pagoda::nodeid() == 0){
        return true;
    } else {
        return false;
    }
}


template <class T>
void NodeZeroArray::get_distribution(vector<int64_t> &lo, vector<int64_t> &hi) const
{
    lo.clear();
    hi.clear();
}


template <class T>
void* NodeZeroArray::access()
{
    return value;
}


template <class T>
void* NodeZeroArray::access() const
{
    return value;
}


template <class T>
void NodeZeroArray::release() const
{
    // TODO could verify at this point that all values are the same
}


template <class T>
void NodeZeroArray::release_update()
{
    // TODO could verify at this point that all values are the same
}


template <class T>
void* NodeZeroArray::get() const
{
    return value;
}


template <class T>
void* NodeZeroArray::get(void *buffer, const vector<int64_t> &lo, const vector<int64_t> &hi, const vector<int64_t> &ld) const
{
    ASSERT(lo.empty());
    ASSERT(hi.empty());
    ASSERT(ld.empty());
    return value;
}


template <class T>
void NodeZeroArray::put(void *buffer, const vector<int64_t> &lo, const vector<int64_t> &hi, const vector<int64_t> &ld)
{
    ASSERT(lo.empty());
    ASSERT(hi.empty());
    ASSERT(ld.empty());
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        *((T*)value) = *((T*)(buffer)); \
    } else
#include "DataType.def"
}


template <class T>
void NodeZeroArray::scatter(void *buffer, vector<int64_t> &subscripts)
{
    ASSERT(subscripts.empty());
#define DATATYPE_EXPAND(DT,T) \
    if (DT == type) { \
        *((T*)value) = *((T*)(buffer)); \
    } else
#include "DataType.def"
}


template <class T>
void* NodeZeroArray::gather(vector<int64_t> &subscripts) const
{
    ASSERT(subscripts.empty());
    return value;
}


template <class T>
Array* NodeZeroArray::add(const Array *rhs) const
{
    const NodeZeroArray *array = dynamic_cast<const NodeZeroArray*>(rhs);
    if (array) {
        NodeZeroArray *self_copy = new NodeZeroArray(*this);
        (*self_copy) += *array;
        return self_copy;
    }
    ERR("NodeZeroArray::add(Array*) fell through");
}


template <class T>
Array* NodeZeroArray::sub(const Array *rhs) const
{
    const NodeZeroArray *array = dynamic_cast<const NodeZeroArray*>(rhs);
    if (array) {
        NodeZeroArray *self_copy = new NodeZeroArray(*this);
        (*self_copy) -= *array;
        return self_copy;
    }
    ERR("NodeZeroArray::sub(Array*) fell through");
}


template <class T>
Array* NodeZeroArray::mul(const Array *rhs) const
{
    const NodeZeroArray *array = dynamic_cast<const NodeZeroArray*>(rhs);
    if (array) {
        NodeZeroArray *self_copy = new NodeZeroArray(*this);
        (*self_copy) *= *array;
        return self_copy;
    }
    ERR("NodeZeroArray::mul(Array*) fell through");
}


template <class T>
Array* NodeZeroArray::div(const Array *rhs) const
{
    const NodeZeroArray *array = dynamic_cast<const NodeZeroArray*>(rhs);
    if (array) {
        NodeZeroArray *self_copy = new NodeZeroArray(*this);
        (*self_copy) /= *array;
        return self_copy;
    }
    ERR("NodeZeroArray::div(Array*) fell through");
}


template <class T>
Array* NodeZeroArray::max(const Array *rhs) const
{
    NodeZeroArray *self_copy = new NodeZeroArray(*this);
    self_copy->imax(rhs);
    return self_copy;
}


template <class T>
Array* NodeZeroArray::min(const Array *rhs) const
{
    NodeZeroArray *self_copy = new NodeZeroArray(*this);
    self_copy->imin(rhs);
    return self_copy;
}


template <class T>
Array* NodeZeroArray::pow(double exponent) const
{
    NodeZeroArray *self_copy = new NodeZeroArray(*this);
    self_copy->ipow(exponent);
    return self_copy;
}


template <class T>
Array* NodeZeroArray::iadd(const Array *rhs)
{
    const NodeZeroArray *array = dynamic_cast<const NodeZeroArray*>(rhs);
    if (array) {
        (*this) += *array;
        return this;
    }
    ERR("NodeZeroArray::iadd(Array*) fell through");
}


template <class T>
Array* NodeZeroArray::isub(const Array *rhs)
{
    const NodeZeroArray *array = dynamic_cast<const NodeZeroArray*>(rhs);
    if (array) {
        (*this) -= *array;
        return this;
    }
    ERR("NodeZeroArray::isub(Array*) fell through");
}


template <class T>
Array* NodeZeroArray::imul(const Array *rhs)
{
    const NodeZeroArray *array = dynamic_cast<const NodeZeroArray*>(rhs);
    if (array) {
        (*this) *= *array;
        return this;
    }
    ERR("NodeZeroArray::imul(Array*) fell through");
}


template <class T>
Array* NodeZeroArray::idiv(const Array *rhs)
{
    const NodeZeroArray *array = dynamic_cast<const NodeZeroArray*>(rhs);
    if (array) {
        (*this) /= *array;
        return this;
    }
    ERR("NodeZeroArray::idiv(Array*) fell through");
}


template <class T>
Array* NodeZeroArray::imax(const Array *rhs)
{
    const NodeZeroArray *array = dynamic_cast<const NodeZeroArray*>(rhs);
    if (array) {
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        T *this_value = (T*)value; \
        T that_value = array->as<T>(); \
        *this_value = (*this_value)>that_value ? *this_value : that_value; \
    } else
#include "DataType.def"
        return this;
    }
    ERR("NodeZeroArray::idiv(Array*) fell through");
}


template <class T>
Array* NodeZeroArray::imin(const Array *rhs)
{
    const NodeZeroArray *array = dynamic_cast<const NodeZeroArray*>(rhs);
    if (array) {
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        T *this_value = (T*)value; \
        T that_value = array->as<T>(); \
        *this_value = (*this_value)<that_value ? *this_value : that_value; \
    } else
#include "DataType.def"
        return this;
    }
    ERR("NodeZeroArray::idiv(Array*) fell through");
}


template <class T>
Array* NodeZeroArray::ipow(double exponent)
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        T *this_value = (T*)value; \
        *this_value = static_cast<T>(std::pow(static_cast<double>(*this_value),exponent)); \
    } else
#include "DataType.def"
    return this;
}


template <class T>
NodeZeroArray& NodeZeroArray::operator+=(const NodeZeroArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        *((T*)value) += *((T*)that.value); \
    } else
#include "DataType.def"
}


template <class T>
NodeZeroArray& NodeZeroArray::operator-=(const NodeZeroArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        *((T*)value) -= *((T*)that.value); \
    } else
#include "DataType.def"
}


template <class T>
NodeZeroArray& NodeZeroArray::operator*=(const NodeZeroArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        *((T*)value) *= *((T*)that.value); \
    } else
#include "DataType.def"
}


template <class T>
NodeZeroArray& NodeZeroArray::operator/=(const NodeZeroArray &that)
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        *((T*)value) /= *((T*)that.value); \
    } else
#include "DataType.def"
}


template <class T>
ostream& NodeZeroArray::print(ostream &os) const
{
    os << "NodeZeroArray";
    return os;
}


template <class T>
void NodeZeroArray::dump() const
{
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        std::ostringstream os; \
        os << *((T*)value) << std::endl; \
        pagoda::print_zero(os.str()); \
    } else
#include "DataType.def"
}

#endif /* NODEZEROARRAY_H_ */
