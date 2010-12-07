#ifndef ABSTRACTARRAY_H_
#define ABSTRACTARRAY_H_

#include <vector>

#include "Array.H"
#include "DataType.H"
#include "Dimension.H"
#include "Error.H"

using std::vector;


/**
 * Abstract implementation of Array interface.
 *
 * As many Array methods as possible are implemented here if they could be
 * implemented using the Array interface's own methods.
 */
class AbstractArray : public Array
{
    public:
        /** Destroys the AbstractArray. */
        virtual ~AbstractArray();

        /** @copydoc Array::get_type() const */
        virtual DataType get_type() const = 0;

        /** @copydoc Array::get_shape() const */
        virtual vector<int64_t> get_shape() const = 0;

        /** @copydoc Array::get_size() const */
        virtual int64_t get_size() const;

        /** @copydoc Array::get_local_shape() const */
        virtual vector<int64_t> get_local_shape() const = 0;

        /** @copydoc Array::get_local_size() const */
        virtual int64_t get_local_size() const;

        /** @copydoc Array::get_ndim() const */
        virtual int64_t get_ndim() const;

        /** @copydoc Array::is_scalar() const */
        virtual bool is_scalar() const;

        /** @copydoc Array::fill(void*) */
        virtual void fill(void *value);

        /** @copydoc Array::copy(const Array*) */
        virtual void copy(const Array *src);

        /** @copydoc Array::copy(const Array*,const vector<int64_t>&,const vector<int64_t>&,const vector<int64_t>&,const vector<int64_t>&) */
        virtual void copy(const Array *src,
                          const vector<int64_t> &src_lo,
                          const vector<int64_t> &src_hi,
                          const vector<int64_t> &dst_lo,
                          const vector<int64_t> &dst_hi) = 0;

        /** @copydoc Array::same_distribution(const Array*) const */
        virtual bool same_distribution(const Array *other) const;

        /** @copydoc Array::owns_data() const */
        virtual bool owns_data() const;

        /** @copydoc Array::get_distribution(vector<int64_t>&,vector<int64_t>&) const */
        virtual void get_distribution(vector<int64_t> &lo,
                                      vector<int64_t> &hi) const = 0;

        /** @copydoc Array::access() */
        virtual void* access() = 0;

        /** @copydoc Array::access() const */
        virtual void* access() const = 0;

        /** @copydoc Array::release() const */
        virtual void release() const = 0;

        /** @copydoc Array::release_update() */
        virtual void release_update() = 0;

        /** @copydoc Array::get(void*) const */
        virtual void* get(void *buffer=NULL) const = 0;

        /** @copydoc Array::get(int64_t,int64_t,void*) const */
        virtual void* get(int64_t lo, int64_t hi, void *buffer=NULL) const = 0;

        /** @copydoc Array::get(const vector<int64_t>&,const vector<int64_t>&,void*) const */
        virtual void* get(const vector<int64_t> &lo,
                          const vector<int64_t> &hi,
                          void *buffer=NULL) const = 0;

        /** @copydoc Array::put(void*) */
        virtual void put(void *buffer) = 0;

        /** @copydoc Array::put(void*,int64_t,int64_t) */
        virtual void put(void *buffer, int64_t lo, int64_t hi) = 0;

        /** @copydoc Array::put(void*,const vector<int64_t>&,const vector<int64_t>&) */
        virtual void put(void *buffer,
                         const vector<int64_t> &lo,
                         const vector<int64_t> &hi) = 0;

        /** @copydoc Array::scatter(void*,vector<int64_t>&) */
        virtual void scatter(void *buffer, vector<int64_t> &subscripts) = 0;

        /** @copydoc Array::gather(vector<int64_t>&,void*) const */
        virtual void* gather(vector<int64_t> &subscripts,
                             void *buffer=NULL) const = 0;

        /** @copydoc Array::add(const Array*) const */
        virtual Array* add(const Array *rhs) const = 0;

        /** @copydoc Array::iadd(const Array*) */
        virtual Array* iadd(const Array *rhs) = 0;

        /** @copydoc Array::sub(const Array*) const */
        virtual Array* sub(const Array *rhs) const = 0;

        /** @copydoc Array::isub(const Array*) */
        virtual Array* isub(const Array *rhs) = 0;

        /** @copydoc Array::mul(const Array*) const */
        virtual Array* mul(const Array *rhs) const = 0;

        /** @copydoc Array::imul(const Array*) */
        virtual Array* imul(const Array *rhs) = 0;

        /** @copydoc Array::div(const Array*) const */
        virtual Array* div(const Array *rhs) const = 0;

        /** @copydoc Array::idiv(const Array*) */
        virtual Array* idiv(const Array *rhs) = 0;

        /** @copydoc Array::max(const Array*) const */
        virtual Array* max(const Array *rhs) const = 0;

        /** @copydoc Array::imax(const Array*) */
        virtual Array* imax(const Array *rhs) = 0;

        /** @copydoc Array::min(const Array*) const */
        virtual Array* min(const Array *rhs) const = 0;

        /** @copydoc Array::imin(const Array*) */
        virtual Array* imin(const Array *rhs) = 0;

        /** @copydoc Array::pow(double) const */
        virtual Array* pow(double exponent) const = 0;

        /** @copydoc Array::ipow(double) */
        virtual Array* ipow(double exponent) = 0;

        /** @copydoc Array::set_fill_value(double) */
        virtual void set_fill_value(double value);

        /** @copydoc Array::has_fill_value() const */
        virtual bool has_fill_value() const;

        /** @copydoc Array::get_fill_value() const */
        virtual double get_fill_value() const;

        /** @copydoc Array::clear_fill_value() */
        virtual void clear_fill_value();

        /** @copydoc Array::set_counter(Array*) */
        virtual void set_counter(Array *counter);

        /** @copydoc Array::print(ostream&) const */
        virtual ostream& print(ostream &os) const = 0;

        /** @copydoc Array::dump() const */
        virtual void dump() const = 0;

    protected:
        /** Constructs the AbstractArray. */
        AbstractArray();

        /**
         * Constructs the AbstractArray from a copy.
         *
         * @param[in] that the AbstractArray to copy from
         */
        AbstractArray(const AbstractArray &that);

        /**
         * @todo document
         *
         * @param[in] rhs @todo document
         * @return @todo document
         */
        bool broadcast_check(const Array *rhs) const;

        bool _has_fill_value; /**< @todo document */
        double _fill_value; /**< @todo document */
        Array *counter; /**< @todo document */
};

#endif /* ABSTRACTARRAY_H_ */