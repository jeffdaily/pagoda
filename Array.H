#ifndef ARRAY_H_
#define ARRAY_H_

#include <vector>

using std::vector;

#include "DataType.H"
#include "Dimension.H"


/**
 * N-dimensional distributed Array interface.
 *
 * Some of the methods of Array assume the data is distributed.  Each process
 * may "own" a portion of the Array's data.  Data is assumed to be distributed
 * in some form of cartesian distribution.  When accessed, the local data can
 * be manipulated as a 1D C array.  This is how new operations may be written.
 */
class Array
{
    public:
        static Array* create(DataType type, vector<int64_t> shape);
        static Array* create(DataType type, vector<Dimension*> dims);

        virtual ~Array();

        virtual DataType get_type() const = 0;
        virtual vector<int64_t> get_shape() const = 0;
        virtual int64_t get_size() const;
        virtual vector<int64_t> get_local_shape() const = 0;
        virtual int64_t get_local_size() const;
        virtual int64_t get_ndim() const = 0;

        virtual void fill(int value) = 0;
        virtual void fill(long value) = 0;
        virtual void fill(long long value) = 0;
        virtual void fill(float value) = 0;
        virtual void fill(double value) = 0;
        virtual void fill(long double value) = 0;

        /**
         * Copies the entire contents of the given Array into this Array.
         * 
         * @param src the Array to copy
         */
        virtual void copy(const Array *src) = 0;

        /**
         * Copies elements in a patch of one Array into another one.
         *
         * The patches of Arrays may be of different shapes but must have the
         * same number of elements.  Patches must be nonoverlapping
         * (if src == dst).
         * 
         * @param src the Array to copy
         * @param src_lo lower bounds of src Array
         * @param src_hi upper bounds of src Array
         * @param dst_lo lower bounds of dst Array
         * @param dst_hi upper bounds of dst Array
         */
        virtual void copy(const Array *src,
                const vector<int64_t> &src_lo,
                const vector<int64_t> &src_hi,
                const vector<int64_t> &dst_lo,
                const vector<int64_t> &dst_hi) = 0;

        virtual bool same_distribution(const Array *other);

        virtual bool owns_data() const = 0;
        virtual void get_distribution(
                vector<int64_t> &lo, vector<int64_t> &hi) const;
        virtual void* access() = 0;
        virtual void* access() const = 0;
        virtual void release() const = 0;
        virtual void release_update() = 0;

        virtual void* get(const vector<int64_t> &lo,
                          const vector<int64_t> &hi) const = 0;
        /*
        virtual void get(const vector<int64_t> &lo,
                         const vector<int64_t> &hi,
                         int *buf) const = 0;
        virtual void get(const vector<int64_t> &lo,
                         const vector<int64_t> &hi,
                         long *buf) const = 0;
        virtual void get(const vector<int64_t> &lo,
                         const vector<int64_t> &hi,
                         long long *buf) const = 0;
        virtual void get(const vector<int64_t> &lo,
                         const vector<int64_t> &hi,
                         float *buf) const = 0;
        virtual void get(const vector<int64_t> &lo,
                         const vector<int64_t> &hi,
                         double *buf) const = 0;
        virtual void get(const vector<int64_t> &lo,
                         const vector<int64_t> &hi,
                         long double *buf) const = 0;
        */

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Array *array);

    protected:
        Array();
};

#endif /* ARRAY_H_ */
