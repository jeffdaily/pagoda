#ifndef GLOBALSCALAR_H_
#define GLOBALSCALAR_H_

#include <stdint.h>

#include <vector>

#include "AbstractArray.H"
#include "DataType.H"
#include "Dimension.H"
#include "ProcessGroup.H"

using std::vector;


/**
 * A single value Array without shape, implemented using Global Arrays.
 */
class GlobalScalar : public AbstractArray
{
    public:
        GlobalScalar(DataType type);
        GlobalScalar(DataType type, ProcessGroup group);
        GlobalScalar(const GlobalScalar &that);
        virtual ~GlobalScalar();

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
        virtual GlobalScalar* clone() const;

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

        virtual ostream& print(ostream &os) const;

        virtual void dump() const;

    protected:
        DataType type;
        int handle;
        bool owns;
        ProcessGroup group;
};

#endif /* GLOBALSCALAR_H_ */
