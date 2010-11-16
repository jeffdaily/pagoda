#ifndef SCALARARRAY_H_
#define SCALARARRAY_H_

#include <vector>

#include "Array.H"
#include "DataType.H"
#include "Dimension.H"

using std::vector;


/**
 * A single value but accessed using the Array interface.
 *
 * Useful when wanting to use the virtual operators such as add and
 * broadcasting is desired.
 */
class ScalarArray : public Array
{
    public:
        ScalarArray(DataType type);
        ScalarArray(const ScalarArray &that);
        virtual ~ScalarArray();

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
        virtual void* gather(vector<int64_t> &subscripts,
                             void *buffer=NULL) const;

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
        ScalarArray& operator+=(const ScalarArray &that);
        ScalarArray& operator-=(const ScalarArray &that);
        ScalarArray& operator*=(const ScalarArray &that);
        ScalarArray& operator/=(const ScalarArray &that);

        virtual ostream& print(ostream &os) const;

        virtual void dump() const;

        template <class cast_type> cast_type as() const {
#define DATATYPE_EXPAND(DT,T) \
            if (DT == type) { \
                T the_value = *static_cast<T*>(value); \
                return static_cast<cast_type>(the_value); \
            } else
#include "DataType.def"
        }

    protected:
        DataType type;
        void *value;
};

#endif /* SCALARARRAY_H_ */
