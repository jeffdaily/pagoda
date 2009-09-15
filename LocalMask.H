#ifndef LOCALMASK_H_
#define LOCALMASK_H_

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


class LocalMask : public Mask
{
    public:
        LocalMask(Dimension *dim);
        virtual ~LocalMask();

        virtual void get_data(vector<int> &data);
        virtual void get_data(vector<int> &data, int64_t lo, int64_t hi);
        virtual void clear();
        virtual void adjust(const DimSlice &slice);
        virtual void adjust(const LatLonBox &box, Variable *lat, Variable *lon);
        virtual void adjust(double low, double hi, Variable *var,
                bool bitwise_or=true);
        virtual void recount();
        virtual void reindex();

    protected:
        vector<int> data;
};

#endif // LOCALMASK_H_
