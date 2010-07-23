#ifndef GLOBALARRAY_H_
#define GLOBALARRAY_H_

#include <vector>

#include "Array.H"
#include "DataType.H"
#include "Dimension.H"

using std::vector;


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

        virtual ostream& print(ostream &os) const;

        virtual void dump() const;

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

        int handle;
        DataType type;
        vector<int64_t> shape;
        vector<int64_t> lo;
        vector<int64_t> hi;

        void *scalar;
};

#endif /* GLOBALARRAY_H_ */
