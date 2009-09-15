#ifndef DISTRIBUTEDMASK_H_
#define DISTRIBUTEDMASK_H_

#include <map>
#include <string>
#include <vector>

#include "LatLonBox.H"
#include "Mask.H"
#include "Slice.H"

using std::map;
using std::string;
using std::vector;

class Dimension;
class Variable;


/**
 * Distribute the mask across all available processs.
 */
class DistributedMask : public Mask
{
    public:
        DistributedMask(Dimension *dim, int value=0);
        virtual ~DistributedMask();

        virtual void get_data(vector<int> &data);
        virtual void get_data(vector<int> &data, int64_t lo, int64_t hi);
        virtual void clear();
        virtual void adjust(const DimSlice &slice);
        virtual void adjust(const LatLonBox &box, Variable *lat, Variable *lon);
        virtual void adjust(double low, double hi, Variable *var,
                bool bitwise_or=true);
        virtual void recount();
        virtual void reindex();

        int get_handle() const { return handle; }
        int get_handle_index() const { return handle_index; }

    protected:
        int handle;
        int handle_index;
        int64_t lo;
        int64_t hi;
        vector<int64_t> counts;
};

#endif // DISTRIBUTEDMASK_H_
