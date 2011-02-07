#ifndef ALLNODEARRAY_H_
#define ALLNODEARRAY_H_

#include <stdint.h>

#include <cmath>
#include <vector>

#include "AbstractArray.H"
#include "DataType.H"
#include "Debug.H"
#include "Dimension.H"
#include "Error.H"

using std::vector;


/**
 * An N-dimensional distributed Array with all data replicated on all nodes.
 */
template <class T>
class AllNodeArray : public AbstractArray
{
    public:
        AllNodeArray(DataType type, vector<int64_t> shape);
        AllNodeArray(DataType type, vector<Dimension*> dims);
        AllNodeArray(const AllNodeArray &that);
        virtual ~AllNodeArray();

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
        virtual const void* access() const;
        virtual void release() const;
        virtual void release_update();

        virtual void* get(void *buffer=NULL) const;
        virtual void* get(int64_t lo, int64_t hi, void *buffer=NULL) const;
        virtual void* get(const vector<int64_t> &lo,
                          const vector<int64_t> &hi,
                          void *buffer=NULL) const;
        virtual void put(void *buffer);
        virtual void put(void *buffer, int64_t lo, int64_t hi);
        virtual void put(void *buffer,
                         const vector<int64_t> &lo,
                         const vector<int64_t> &hi);
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
        AllNodeArray& operator+=(const AllNodeArray &that);
        AllNodeArray& operator-=(const AllNodeArray &that);
        AllNodeArray& operator*=(const AllNodeArray &that);
        AllNodeArray& operator/=(const AllNodeArray &that);

        virtual ostream& print(ostream &os) const;

        virtual void dump() const;

    protected:
        DataType type;
        T *data;
        vector<int64_t> shape;
        vector<int64_t> lo;
        vector<int64_t> hi;
};


template <class T>
AllNodeArray<T>::AllNodeArray(DataType type, vector<int64_t> shape)
    :   AbstractArray()
    ,   type(type)
    ,   data(new T[pagoda::shape_to_size(shape)])
    ,   shape(shape)
    ,   lo(shape.size(), 0)
    ,   hi(shape)
{
    // hi is really shape -1
    for (size_t i=0; i<hi.size(); i++) {
        hi[i] -= 1;
    }
}


template <class T>
AllNodeArray<T>::AllNodeArray(const AllNodeArray<T> &that)
    :   AbstractArray(that)
    ,   type(that.type)
    ,   data(new T[pagoda::shape_to_size(that.shape)])
    ,   shape(that.shape)
    ,   lo(that.lo)
    ,   hi(that.hi)
{
    std::copy(that.data, that.data+get_size(), data);
}


template <class T>
AllNodeArray<T>::~AllNodeArray()
{
    delete [] data;
}


template <class T>
DataType AllNodeArray<T>::get_type() const
{
    return type;
}


template <class T>
vector<int64_t> AllNodeArray<T>::get_shape() const
{
    return shape;
}


template <class T>
vector<int64_t> AllNodeArray<T>::get_local_shape() const
{
    return shape;
}


template <class T>
int64_t AllNodeArray<T>::get_ndim() const
{
    return shape.size();
}


template <class T>
void AllNodeArray<T>::fill(void *new_value)
{
    T *value = static_cast<T*>(new_value);
    std::fill(data, data+get_size(), *value);
}


template <class T>
void AllNodeArray<T>::copy(const Array *src)
{
    T *src_data;

    if (type != src->get_type()) {
        ERR("arrays must be same type");
    }
    if (shape != src->get_shape()) {
        ERR("arrays must be same shape");
    }

    src_data = static_cast<T*>(src->get(lo,hi));
    std::copy(src_data, src_data+get_size(), data);
}


template <class T>
void AllNodeArray<T>::copy(const Array *src, const vector<int64_t> &src_lo, const vector<int64_t> &src_hi, const vector<int64_t> &dst_lo, const vector<int64_t> &dst_hi)
{
    ERR("not implemented AllNodeArray<T>::copy lo/hi");
}


template <class T>
bool AllNodeArray<T>::owns_data() const
{
    return true;
}


template <class T>
void AllNodeArray<T>::get_distribution(
    vector<int64_t> &lo, vector<int64_t> &hi) const
{
    lo = this->lo;
    hi = this->hi;
}


template <class T>
void* AllNodeArray<T>::access()
{
    return data;
}


template <class T>
const void* AllNodeArray<T>::access() const
{
    return data;
}


template <class T>
void AllNodeArray<T>::release() const
{
}


template <class T>
void AllNodeArray<T>::release_update()
{
}


template <class T>
void* AllNodeArray<T>::get(void *buffer) const
{
    return data;
}


template <class T>
void* AllNodeArray<T>::get(int64_t lo, int64_t hi, void *buffer) const
{
    ERR("Not Implemented");
}


template <class T>
void* AllNodeArray<T>::get(const vector<int64_t> &lo,
                           const vector<int64_t> &hi, void *buffer) const
{
    ASSERT(lo.empty());
    ASSERT(hi.empty());
    ERR("Not Implemented");
}


template <class T>
void AllNodeArray<T>::put(void *buffer)
{
    ERR("Not Implemented");
}


template <class T>
void AllNodeArray<T>::put(void *buffer, int64_t lo, int64_t hi)
{
    ERR("Not Implemented");
}


template <class T>
void AllNodeArray<T>::put(void *buffer,
                          const vector<int64_t> &lo,
                          const vector<int64_t> &hi)
{
    ASSERT(lo.empty());
    ASSERT(hi.empty());
    ERR("Not Implemented");
}


template <class T>
void AllNodeArray<T>::scatter(void *buffer, vector<int64_t> &subscripts)
{
    ASSERT(subscripts.empty());
    ERR("Not Implemented");
}


template <class T>
void* AllNodeArray<T>::gather(vector<int64_t> &subscripts) const
{
    ASSERT(subscripts.empty());
    ERR("Not Implemented");
}


template <class T>
Array* AllNodeArray<T>::add(const Array *rhs) const
{
    const AllNodeArray *array = dynamic_cast<const AllNodeArray*>(rhs);
    if (array) {
        AllNodeArray *self_copy = new AllNodeArray(*this);
        (*self_copy) += *array;
        return self_copy;
    }
    ERR("AllNodeArray<T>::add(Array*) fell through");
}


template <class T>
Array* AllNodeArray<T>::sub(const Array *rhs) const
{
    const AllNodeArray *array = dynamic_cast<const AllNodeArray*>(rhs);
    if (array) {
        AllNodeArray *self_copy = new AllNodeArray(*this);
        (*self_copy) -= *array;
        return self_copy;
    }
    ERR("AllNodeArray<T>::sub(Array*) fell through");
}


template <class T>
Array* AllNodeArray<T>::mul(const Array *rhs) const
{
    const AllNodeArray *array = dynamic_cast<const AllNodeArray*>(rhs);
    if (array) {
        AllNodeArray *self_copy = new AllNodeArray(*this);
        (*self_copy) *= *array;
        return self_copy;
    }
    ERR("AllNodeArray<T>::mul(Array*) fell through");
}


template <class T>
Array* AllNodeArray<T>::div(const Array *rhs) const
{
    const AllNodeArray *array = dynamic_cast<const AllNodeArray*>(rhs);
    if (array) {
        AllNodeArray *self_copy = new AllNodeArray(*this);
        (*self_copy) /= *array;
        return self_copy;
    }
    ERR("AllNodeArray<T>::div(Array*) fell through");
}


template <class T>
Array* AllNodeArray<T>::max(const Array *rhs) const
{
    AllNodeArray *self_copy = new AllNodeArray(*this);
    self_copy->imax(rhs);
    return self_copy;
}


template <class T>
Array* AllNodeArray<T>::min(const Array *rhs) const
{
    AllNodeArray *self_copy = new AllNodeArray(*this);
    self_copy->imin(rhs);
    return self_copy;
}


template <class T>
Array* AllNodeArray<T>::pow(double exponent) const
{
    AllNodeArray *self_copy = new AllNodeArray(*this);
    self_copy->ipow(exponent);
    return self_copy;
}


template <class T>
Array* AllNodeArray<T>::iadd(const Array *rhs)
{
    const AllNodeArray *array = dynamic_cast<const AllNodeArray*>(rhs);
    if (array) {
        (*this) += *array;
        return this;
    }
    ERR("AllNodeArray<T>::iadd(Array*) fell through");
}


template <class T>
Array* AllNodeArray<T>::isub(const Array *rhs)
{
    const AllNodeArray *array = dynamic_cast<const AllNodeArray*>(rhs);
    if (array) {
        (*this) -= *array;
        return this;
    }
    ERR("AllNodeArray<T>::isub(Array*) fell through");
}


template <class T>
Array* AllNodeArray<T>::imul(const Array *rhs)
{
    const AllNodeArray *array = dynamic_cast<const AllNodeArray*>(rhs);
    if (array) {
        (*this) *= *array;
        return this;
    }
    ERR("AllNodeArray<T>::imul(Array*) fell through");
}


template <class T>
Array* AllNodeArray<T>::idiv(const Array *rhs)
{
    const AllNodeArray *array = dynamic_cast<const AllNodeArray*>(rhs);
    if (array) {
        (*this) /= *array;
        return this;
    }
    ERR("AllNodeArray<T>::idiv(Array*) fell through");
}


template <class T>
Array* AllNodeArray<T>::imax(const Array *rhs)
{
    ERR("Not Implemented");
}


template <class T>
Array* AllNodeArray<T>::imin(const Array *rhs)
{
    ERR("Not Implemented");
}


template <class T>
Array* AllNodeArray<T>::ipow(double exponent)
{
    ERR("Not Implemented");
}


template <class T>
AllNodeArray<T>& AllNodeArray<T>::operator+=(const AllNodeArray<T> &that)
{
    ERR("Not Implemented");
}


template <class T>
AllNodeArray<T>& AllNodeArray<T>::operator-=(const AllNodeArray<T> &that)
{
    ERR("Not Implemented");
}


template <class T>
AllNodeArray<T>& AllNodeArray<T>::operator*=(const AllNodeArray<T> &that)
{
    ERR("Not Implemented");
}


template <class T>
AllNodeArray<T>& AllNodeArray<T>::operator/=(const AllNodeArray<T> &that)
{
    ERR("Not Implemented");
}


template <class T>
ostream& AllNodeArray<T>::print(ostream &os) const
{
    os << "AllNodeArray";
    return os;
}


template <class T>
void AllNodeArray<T>::dump() const
{
    ERR("Not Implemented");
}

#endif /* ALLNODEARRAY_H_ */
