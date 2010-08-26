#ifndef SCALARARRAY_H_
#define SCALARARRAY_H_

#include <vector>

#include "Array.H"
#include "DataType.H"
#include "Dimension.H"

using std::vector;


/**
 * An N-dimensional distributed Array based on the Global Arrays toolkit.
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

        virtual void fill(int value);
        virtual void fill(long value);
        virtual void fill(long long value);
        virtual void fill(float value);
        virtual void fill(double value);
        virtual void fill(long double value);

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
        virtual Array* iadd(const Array *rhs);
        virtual Array* isub(const Array *rhs);
        virtual Array* imul(const Array *rhs);
        virtual Array* idiv(const Array *rhs);
        ScalarArray& operator+=(const ScalarArray &that);
        ScalarArray& operator-=(const ScalarArray &that);
        ScalarArray& operator*=(const ScalarArray &that);
        ScalarArray& operator/=(const ScalarArray &that);

        virtual ostream& print(ostream &os) const;

        virtual void dump() const;

    protected:
        DataType type;
        void *value;
};

#endif /* SCALARARRAY_H_ */
