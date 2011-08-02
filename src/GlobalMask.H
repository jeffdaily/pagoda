#ifndef GLOBALMASK_H_
#define GLOBALMASK_H_

#include <stdint.h>

#include <string>

#include "GlobalArray.H"
#include "IndexHyperslab.H"
#include "LatLonBox.H"
#include "Mask.H"

using std::string;

class Array;
class Dimension;
class Variable;


/**
 * A Dimension Mask implemented using a GlobalArray.
 */
class GlobalMask : public Mask
{
    public:
        GlobalMask(const string &name, int64_t size);
        GlobalMask(const GlobalMask &that);
        virtual ~GlobalMask();

        // Mask virtuals
        virtual string get_name() const;
        virtual int64_t get_count();
        virtual void clear();
        virtual void reset();

        virtual void modify(const IndexHyperslab &hyperslab);
        virtual void modify(const LatLonBox &box,
                            const Array *lat, const Array *lon);
        virtual void modify(double low, double hi, const Array *var);
        virtual void modify_gt(double value, const Array *var);
        virtual void modify_lt(double value, const Array *var);

        virtual Array* reindex();
        virtual Array* partial_sum(bool exclude);

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
        virtual GlobalMask* clone() const;

        virtual bool same_distribution(const Array *other) const;

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
                          void *buffer) const;
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

        // shared virtuals
        virtual ostream& print(ostream &os) const;

        virtual void dump() const;

        // non-virtuals
        void normalize();

    protected:
        GlobalArray *mask;
        Array *index;
        Array *sum;
        int64_t count;
        bool last_excl;
        bool dirty_index;
        bool dirty_sum;
        bool dirty_count;
        string name;
};

#endif /* GLOBALMASK_H_ */
