#ifndef NODEZEROARRAY_H_
#define NODEZEROARRAY_H_

#include <cmath>
#include <vector>

#include "Array.H"
#include "Bootstrap.H"
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
        NodeZeroArray(const vector<int64_t> &shape);
        NodeZeroArray(const NodeZeroArray<T> &that);
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

        NodeZeroArray<T>& operator+=(const NodeZeroArray<T> &that);
        NodeZeroArray<T>& operator+=(const ScalarArray &that);
        NodeZeroArray<T>& operator-=(const NodeZeroArray<T> &that);
        NodeZeroArray<T>& operator-=(const ScalarArray &that);
        NodeZeroArray<T>& operator*=(const NodeZeroArray<T> &that);
        NodeZeroArray<T>& operator*=(const ScalarArray &that);
        NodeZeroArray<T>& operator/=(const NodeZeroArray<T> &that);
        NodeZeroArray<T>& operator/=(const ScalarArray &that);

        virtual ostream& print(ostream &os) const;

        virtual void dump() const;

    protected:
        DataType type;
        T *data;
        vector<int64_t> shape;
        vector<int64_t> lo;
        vector<int64_t> hi;
        int64_t size;
};


template <class T>
NodeZeroArray<T>::NodeZeroArray(const vector<int64_t> &shape)
    :   Array()
    ,   type(DataType::ctype<T>())
    ,   data(NULL)
    ,   shape(shape)
    ,   lo()
    ,   hi()
    ,   size(pagoda::shape_to_size(shape))
{
    if (0 == pagoda::me) {
        data = new T[size];
        lo.assign(shape.size(), 0);
        hi = shape;
        // hi is really shape -1
        for (size_t i=0; i<hi.size(); ++i) {
            hi[i] -= 1;
        }
    }
    else {
        lo.assign(shape.size(), -1);
        hi.assign(shape.size(), -2);
    }
}


template <class T>
NodeZeroArray<T>::NodeZeroArray(const NodeZeroArray<T> &that)
    :   Array()
    ,   type(that.type)
    ,   data(NULL)
    ,   shape(that.shape)
    ,   lo(that.lo)
    ,   hi(that.hi)
    ,   size(that.size)
{
    if (0 == pagoda::me) {
        data = new T[size];
        std::copy(that.data, that.data+size, data);
    }
}


template <class T>
NodeZeroArray<T>::~NodeZeroArray()
{
    if (0 == pagoda::me) {
        delete [] data;
        data = NULL;
    }
}


template <class T>
DataType NodeZeroArray<T>::get_type() const
{
    return type;
}


template <class T>
vector<int64_t> NodeZeroArray<T>::get_shape() const
{
    return shape;
}


template <class T>
vector<int64_t> NodeZeroArray<T>::get_local_shape() const
{
    return shape;
}


template <class T>
int64_t NodeZeroArray<T>::get_ndim() const
{
    return shape.size();
}


template <class T>
void NodeZeroArray<T>::fill(void *new_value)
{
    T value = *(static_cast<T*>(new_value));
    std::fill(data, data+size, value);
}


template <class T>
void NodeZeroArray<T>::copy(const Array *src)
{
    const NodeZeroArray<T> *sa_src = dynamic_cast<const NodeZeroArray<T>*>(src);
    if (type != src->get_type()) {
        ERR("arrays must be same type");
    }
    if (get_shape() != src->get_shape()) {
        ERR("arrays must be same shape");
    }
    if (sa_src) {
        // optimized local copy
        if (0 == pagoda::me) {
            std::copy(sa_src->data, sa_src->data+size, data);
        }
    }
    else {
        // passed Array is not a NodeZeroArray -- this is a full copy to node
        // zero which could be expensive in terms of memory on that node
        if (0 == pagoda::me) {
            if (data) {
                delete [] data;
                data = NULL;
            }
            data = static_cast<T*>(src->get(lo, hi));
        }
    }
}


template <class T>
void NodeZeroArray<T>::copy(const Array *src, const vector<int64_t> &src_lo, const vector<int64_t> &src_hi, const vector<int64_t> &dst_lo, const vector<int64_t> &dst_hi)
{
    ERR("not implemented NodeZeroArray<T>::copy");
}


template <class T>
bool NodeZeroArray<T>::owns_data() const
{
    if (pagoda::nodeid() == 0) {
        return true;
    }
    else {
        return false;
    }
}


template <class T>
void NodeZeroArray<T>::get_distribution(vector<int64_t> &lo, vector<int64_t> &hi) const
{
    lo = this->lo;
    hi = this->hi;
}


template <class T>
void* NodeZeroArray<T>::access()
{
    return data;
}


template <class T>
void* NodeZeroArray<T>::access() const
{
    return data;
}


template <class T>
void NodeZeroArray<T>::release() const
{
}


template <class T>
void NodeZeroArray<T>::release_update()
{
}


template <class T>
void* NodeZeroArray<T>::get() const
{
    return data;
}


template <class T>
void* NodeZeroArray<T>::get(void *buffer, const vector<int64_t> &lo, const vector<int64_t> &hi, const vector<int64_t> &ld) const
{
    // TODO
    ERR("not implemented NodeZeroArray<T>::get");
}


template <class T>
void NodeZeroArray<T>::put(void *buffer, const vector<int64_t> &lo, const vector<int64_t> &hi, const vector<int64_t> &ld)
{
    // TODO
    ERR("not implemented NodeZeroArray<T>::put");
}


template <class T>
void NodeZeroArray<T>::scatter(void *buffer, vector<int64_t> &subscripts)
{
    // TODO
    ERR("not implemented NodeZeroArray<T>::scatter");
}


template <class T>
void* NodeZeroArray<T>::gather(vector<int64_t> &subscripts) const
{
    // TODO
    ERR("not implemented NodeZeroArray<T>::gather");
}


template <class T>
Array* NodeZeroArray<T>::add(const Array *rhs) const
{
    const NodeZeroArray<T> *array;
    const ScalarArray *scalar;
    NodeZeroArray<T> *self_copy;

    if ((array = dynamic_cast<const NodeZeroArray<T>*>(rhs))) {
        self_copy = new NodeZeroArray<T>(*this);
        (*self_copy) += *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        self_copy = new NodeZeroArray<T>(*this);
        (*self_copy) += *scalar;
    }
    else {
        ERR("NodeZeroArray<T>::add(Array*) fell through");
    }

    return self_copy;
}


template <class T>
Array* NodeZeroArray<T>::sub(const Array *rhs) const
{
    const NodeZeroArray<T> *array;
    const ScalarArray *scalar;
    NodeZeroArray<T> *self_copy;

    if ((array = dynamic_cast<const NodeZeroArray<T>*>(rhs))) {
        self_copy = new NodeZeroArray<T>(*this);
        (*self_copy) -= *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        self_copy = new NodeZeroArray<T>(*this);
        (*self_copy) -= *scalar;
    }
    else {
        ERR("NodeZeroArray<T>::sub(Array*) fell through");
    }

    return self_copy;
}


template <class T>
Array* NodeZeroArray<T>::mul(const Array *rhs) const
{
    const NodeZeroArray<T> *array;
    const ScalarArray *scalar;
    NodeZeroArray<T> *self_copy;

    if ((array = dynamic_cast<const NodeZeroArray<T>*>(rhs))) {
        self_copy = new NodeZeroArray<T>(*this);
        (*self_copy) *= *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        self_copy = new NodeZeroArray<T>(*this);
        (*self_copy) *= *scalar;
    }
    else {
        ERR("NodeZeroArray<T>::mul(Array*) fell through");
    }

    return self_copy;
}


template <class T>
Array* NodeZeroArray<T>::div(const Array *rhs) const
{
    const NodeZeroArray<T> *array;
    const ScalarArray *scalar;
    NodeZeroArray<T> *self_copy;

    if ((array = dynamic_cast<const NodeZeroArray<T>*>(rhs))) {
        self_copy = new NodeZeroArray<T>(*this);
        (*self_copy) /= *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        self_copy = new NodeZeroArray<T>(*this);
        (*self_copy) /= *scalar;
    }
    else {
        ERR("NodeZeroArray<T>::div(Array*) fell through");
    }

    return self_copy;
}


template <class T>
Array* NodeZeroArray<T>::max(const Array *rhs) const
{
    NodeZeroArray<T> *self_copy = new NodeZeroArray<T>(*this);
    self_copy->imax(rhs);
    return self_copy;
}


template <class T>
Array* NodeZeroArray<T>::min(const Array *rhs) const
{
    NodeZeroArray<T> *self_copy = new NodeZeroArray<T>(*this);
    self_copy->imin(rhs);
    return self_copy;
}


template <class T>
Array* NodeZeroArray<T>::pow(double exponent) const
{
    NodeZeroArray<T> *self_copy = new NodeZeroArray<T>(*this);
    self_copy->ipow(exponent);
    return self_copy;
}


template <class T>
Array* NodeZeroArray<T>::iadd(const Array *rhs)
{
    const NodeZeroArray<T> *array;
    const ScalarArray *scalar;

    if ((array = dynamic_cast<const NodeZeroArray<T>*>(rhs))) {
        *this += *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        *this += *scalar;
    }
    else {
        ERR("NodeZeroArray<T>::iadd(Array*) fell through");
    }

    return this;
}


template <class T>
Array* NodeZeroArray<T>::isub(const Array *rhs)
{
    const NodeZeroArray<T> *array;
    const ScalarArray *scalar;

    if ((array = dynamic_cast<const NodeZeroArray<T>*>(rhs))) {
        *this -= *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        *this -= *scalar;
    }
    else {
        ERR("NodeZeroArray<T>::isub(Array*) fell through");
    }

    return this;
}


template <class T>
Array* NodeZeroArray<T>::imul(const Array *rhs)
{
    const NodeZeroArray<T> *array;
    const ScalarArray *scalar;

    if ((array = dynamic_cast<const NodeZeroArray<T>*>(rhs))) {
        *this *= *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        *this *= *scalar;
    }
    else {
        ERR("NodeZeroArray<T>::imul(Array*) fell through");
    }

    return this;
}


template <class T>
Array* NodeZeroArray<T>::idiv(const Array *rhs)
{
    const NodeZeroArray<T> *array;
    const ScalarArray *scalar;

    if ((array = dynamic_cast<const NodeZeroArray<T>*>(rhs))) {
        *this /= *array;
    }
    else if ((scalar = dynamic_cast<const ScalarArray*>(rhs))) {
        *this /= *scalar;
    }
    else {
        ERR("NodeZeroArray<T>::idiv(Array*) fell through");
    }

    return this;
}


template <class T>
Array* NodeZeroArray<T>::imax(const Array *rhs)
{
    const NodeZeroArray<T> *array = dynamic_cast<const NodeZeroArray<T>*>(rhs);
    if (array) {
        // TODO
        ERR("not implemented NodeZeroArray<T>::imax");
    }
    ERR("NodeZeroArray<T>::idiv(Array*) fell through");
}


template <class T>
Array* NodeZeroArray<T>::imin(const Array *rhs)
{
    const NodeZeroArray<T> *array = dynamic_cast<const NodeZeroArray<T>*>(rhs);
    if (array) {
        // TODO
        ERR("not implemented NodeZeroArray<T>::imin");
    }
    ERR("NodeZeroArray<T>::idiv(Array*) fell through");
}


template <class T>
Array* NodeZeroArray<T>::ipow(double exponent)
{
    if (0 == pagoda::me) {
        for (int64_t i=0; i<size; ++i) {
            data[i] = static_cast<T>(std::pow(data[i], exponent));
        }
    }
    return this;
}


template <class T>
NodeZeroArray<T>& NodeZeroArray<T>::operator+=(const NodeZeroArray<T> &that)
{
    if (0 == pagoda::me) {
        for (int64_t i=0; i<size; ++i) {
            data[i] += that.data[i];
        }
    }
    return *this;
}


template <class T>
NodeZeroArray<T>& NodeZeroArray<T>::operator+=(const ScalarArray &that)
{
    if (0 == pagoda::me) {
        T value = that.as<T>();
        for (int64_t i=0; i<size; ++i) {
            data[i] += value;
        }
    }
    return *this;
}


template <class T>
NodeZeroArray<T>& NodeZeroArray<T>::operator-=(const NodeZeroArray<T> &that)
{
    if (0 == pagoda::me) {
        for (int64_t i=0; i<size; ++i) {
            data[i] -= that.data[i];
        }
    }
    return *this;
}


template <class T>
NodeZeroArray<T>& NodeZeroArray<T>::operator-=(const ScalarArray &that)
{
    if (0 == pagoda::me) {
        T value = that.as<T>();
        for (int64_t i=0; i<size; ++i) {
            data[i] -= value;
        }
    }
    return *this;
}


template <class T>
NodeZeroArray<T>& NodeZeroArray<T>::operator*=(const NodeZeroArray<T> &that)
{
    if (0 == pagoda::me) {
        for (int64_t i=0; i<size; ++i) {
            data[i] *= that.data[i];
        }
    }
    return *this;
}


template <class T>
NodeZeroArray<T>& NodeZeroArray<T>::operator*=(const ScalarArray &that)
{
    if (0 == pagoda::me) {
        T value = that.as<T>();
        for (int64_t i=0; i<size; ++i) {
            data[i] *= value;
        }
    }
    return *this;
}


template <class T>
NodeZeroArray<T>& NodeZeroArray<T>::operator/=(const NodeZeroArray<T> &that)
{
    if (0 == pagoda::me) {
        for (int64_t i=0; i<size; ++i) {
            data[i] /= that.data[i];
        }
    }
    return *this;
}


template <class T>
NodeZeroArray<T>& NodeZeroArray<T>::operator/=(const ScalarArray &that)
{
    if (0 == pagoda::me) {
        T value = that.as<T>();
        for (int64_t i=0; i<size; ++i) {
            data[i] /= value;
        }
    }
    return *this;
}


template <class T>
ostream& NodeZeroArray<T>::print(ostream &os) const
{
    os << "NodeZeroArray";
    return os;
}


template <class T>
void NodeZeroArray<T>::dump() const
{
    // TODO
    ERR("not implemented NodeZeroArray<T>::dump");
}

#endif /* NODEZEROARRAY_H_ */
