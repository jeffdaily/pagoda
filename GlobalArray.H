#ifndef GLOBALARRAY_H_
#define GLOBALARRAY_H_

#include <vector>

using std::vector;

#include "Array.H"
#include "DataType.H"
#include "Dimension.H"


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

        GlobalArray& operator=(int value);
        GlobalArray& operator=(long value);
        GlobalArray& operator=(long long value);
        GlobalArray& operator=(float value);
        GlobalArray& operator=(double value);
        GlobalArray& operator=(long double value);
        GlobalArray& operator=(const GlobalArray &that);

        GlobalArray& operator+=(const GlobalArray &that);
        GlobalArray& operator-=(const GlobalArray &that);
        GlobalArray& operator*=(const GlobalArray &that);
        GlobalArray& operator/=(const GlobalArray &that);

        virtual bool owns_data() const;
        virtual void get_distribution(
                vector<int64_t> &lo, vector<int64_t> &hi) const;
        virtual void* access();
        virtual void* access() const;
        virtual void release() const;
        virtual void release_update();

        virtual void* get(const vector<int64_t> &lo,
                          const vector<int64_t> &hi) const;

        virtual ostream& print(ostream &os) const;

    protected:
        void set_distribution();

        int handle;
        DataType type;
        vector<int64_t> shape;
        vector<int64_t> lo;
        vector<int64_t> hi;
};

#endif /* GLOBALARRAY_H_ */
