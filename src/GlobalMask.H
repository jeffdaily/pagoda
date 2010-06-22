#ifndef GLOBALMASK_H_
#define GLOBALMASK_H_

#include <string>

#include "GlobalArray.H"
#include "LatLonBox.H"
#include "Mask.H"
#include "Slice.H"

using std::string;

class Array;
class Dimension;
class Variable;


/**
 * A Dimension Mask implemented using a GlobalArray.
 */
class GlobalMask :  public Mask
{
    public:
        GlobalMask(const Dimension *dim);
        GlobalMask(const GlobalMask &that);
        virtual ~GlobalMask();

        // Mask virtuals
        virtual int64_t get_count() const;
        virtual void clear();
        virtual void reset();

        virtual void modify(const DimSlice &slice);
        virtual void modify(const LatLonBox &box,
                            const Array *lat, const Array *lon);
        virtual void modify(double low, double hi, const Array *var);

        virtual Array* reindex() const;
        virtual Array* partial_sum(bool exclude) const;

        // Array forwards and virtuals
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

        virtual bool same_distribution(const Array *other);

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
        virtual void scatter(void *buffer, const vector<int64_t> &subscripts);
        virtual void* gather(const vector<int64_t> &subscripts) const;

        // shared virtuals
        virtual ostream& print(ostream &os) const;
        
        virtual void dump() const;

        // non-virtuals 
        void normalize();

    protected:
        GlobalArray *mask;
};

#endif /* GLOBALMASK_H_ */
