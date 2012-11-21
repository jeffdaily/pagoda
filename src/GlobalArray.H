#ifndef GLOBALARRAY_H_
#define GLOBALARRAY_H_

#include <stdint.h>

#include <vector>

#include "AbstractArray.H"
#include "DataType.H"
#include "Dimension.H"
#include "ProcessGroup.H"
#include "ScalarArray.H"

using std::vector;

class GlobalArray;
typedef void (GlobalArray::*GA_op_whole)(int);
typedef void (GlobalArray::*GA_op_patch)(int,
        vector<int64_t>&,vector<int64_t>&, vector<int64_t>&,vector<int64_t>&);


/**
 * An N-dimensional distributed Array based on the Global Arrays toolkit.
 */
class GlobalArray : public AbstractArray
{
    public:
        /**
         * Convert DataType to Global Arrays type.
         *
         * @return the Global Arrays type
         */
        static int to_ga(const DataType &type);

        /**
         * Convert Global Arrays type to DataType.
         *
         * @return the DataType
         */
        static DataType to_dt(int type);

        GlobalArray(DataType type, vector<int64_t> shape);
        GlobalArray(DataType type, vector<int64_t> shape,
                const ProcessGroup &group);
        GlobalArray(DataType type, vector<Dimension*> dims);
        GlobalArray(DataType type, vector<Dimension*> dims,
                const ProcessGroup &group);
        GlobalArray(const GlobalArray &that);
        virtual ~GlobalArray();

        virtual DataType get_type() const;
        virtual vector<int64_t> get_shape() const;
        virtual vector<int64_t> get_local_shape() const;
        virtual int64_t get_ndim() const;

        virtual void fill(void *value);

#if 0
        virtual void copy(const Array *src);
#endif
        virtual void copy(const Array *src,
                          const vector<int64_t> &src_lo,
                          const vector<int64_t> &src_hi,
                          const vector<int64_t> &dst_lo,
                          const vector<int64_t> &dst_hi);
        virtual GlobalArray* clone() const;

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
        virtual void acc(void *buffer, void *alpha=NULL);
        virtual void acc(void *buffer, int64_t lo, int64_t hi,
                         void *alpha=NULL);
        virtual void acc(void *buffer,
                         const vector<int64_t> &lo,
                         const vector<int64_t> &hi,
                         void *alpha=NULL);
        virtual void scatter(void *buffer, vector<int64_t> &subscripts);
        virtual void* gather(vector<int64_t> &subscripts,
                             void *buffer=NULL) const;

#if 0
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
#endif

        virtual ostream& print(ostream &os) const;

        virtual void dump() const;

        GlobalArray* cast(DataType type) const;

        GlobalArray& operator=(const GlobalArray &that);

        GlobalArray operator+(const GlobalArray &that) const;
        GlobalArray operator+(const ScalarArray &that) const;
        GlobalArray& operator+=(const GlobalArray &that);
        GlobalArray& operator+=(const ScalarArray &that);

        GlobalArray operator-(const GlobalArray &that) const;
        GlobalArray operator-(const ScalarArray &that) const;
        GlobalArray& operator-=(const GlobalArray &that);
        GlobalArray& operator-=(const ScalarArray &that);

        GlobalArray operator*(const GlobalArray &that) const;
        GlobalArray operator*(const ScalarArray &that) const;
        GlobalArray& operator*=(const GlobalArray &that);
        GlobalArray& operator*=(const ScalarArray &that);

        GlobalArray operator/(const GlobalArray &that) const;
        GlobalArray operator/(const ScalarArray &that) const;
        GlobalArray& operator/=(const GlobalArray &that);
        GlobalArray& operator/=(const ScalarArray &that);

    protected:
        void create();
        void set_distribution();
        void operate(const GlobalArray &that, GA_op_whole, GA_op_patch);
        void operate_add(int that_handle);
        void operate_sub(int that_handle);
        void operate_mul(int that_handle);
        void operate_div(int that_handle);
        void operate_max(int that_handle);
        void operate_min(int that_handle);
        void operate_add_patch(int that_handle,
                               vector<int64_t> &my_lo, vector<int64_t> &my_hi,
                               vector<int64_t> &that_lo, vector<int64_t> &that_hi);
        void operate_sub_patch(int that_handle,
                               vector<int64_t> &my_lo, vector<int64_t> &my_hi,
                               vector<int64_t> &that_lo, vector<int64_t> &that_hi);
        void operate_mul_patch(int that_handle,
                               vector<int64_t> &my_lo, vector<int64_t> &my_hi,
                               vector<int64_t> &that_lo, vector<int64_t> &that_hi);
        void operate_div_patch(int that_handle,
                               vector<int64_t> &my_lo, vector<int64_t> &my_hi,
                               vector<int64_t> &that_lo, vector<int64_t> &that_hi);
        void operate_max_patch(int that_handle,
                               vector<int64_t> &my_lo, vector<int64_t> &my_hi,
                               vector<int64_t> &that_lo, vector<int64_t> &that_hi);
        void operate_min_patch(int that_handle,
                               vector<int64_t> &my_lo, vector<int64_t> &my_hi,
                               vector<int64_t> &that_lo, vector<int64_t> &that_hi);

        int handle;
        DataType type;
        vector<int64_t> shape;
        vector<int64_t> lo;
        vector<int64_t> hi;
        ProcessGroup group;
};

#endif /* GLOBALARRAY_H_ */
