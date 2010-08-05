#ifndef GLOBALARRAY_H_
#define GLOBALARRAY_H_

#include <vector>

#include "Array.H"
#include "DataType.H"
#include "Dimension.H"

using std::vector;

class GlobalArray;
typedef void (GlobalArray::*GA_op_whole)(int);
typedef void (GlobalArray::*GA_op_patch)(int,
        vector<int64_t>&,vector<int64_t>&, vector<int64_t>&,vector<int64_t>&);


/**
 * An N-dimensional distributed Array based on the Global Arrays toolkit.
 */
class GlobalArray : public Array
{
    public:
        GlobalArray(DataType type, vector<int64_t> shape);
        GlobalArray(DataType type, vector<Dimension*> dims);
        GlobalArray(const GlobalArray &that);
        virtual ~GlobalArray();

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

        virtual ostream& print(ostream &os) const;

        virtual void dump() const;

        GlobalArray* cast(DataType type) const;

        GlobalArray& operator=(const GlobalArray &that);

        GlobalArray operator+(const GlobalArray &that);
        GlobalArray operator-(const GlobalArray &that);
        GlobalArray operator*(const GlobalArray &that);
        GlobalArray operator/(const GlobalArray &that);

        GlobalArray& operator+=(const GlobalArray &that);
        GlobalArray& operator-=(const GlobalArray &that);
        GlobalArray& operator*=(const GlobalArray &that);
        GlobalArray& operator/=(const GlobalArray &that);

    protected:
        void create();
        void create_scalar();
        void set_distribution();
        bool is_scalar() const;
        void operate(const GlobalArray &that, GA_op_whole, GA_op_patch);
        void operate_add(int that_handle);
        void operate_sub(int that_handle);
        void operate_mul(int that_handle);
        void operate_div(int that_handle);
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

        int handle;
        DataType type;
        vector<int64_t> shape;
        vector<int64_t> lo;
        vector<int64_t> hi;

        void *scalar;
};

#endif /* GLOBALARRAY_H_ */
