#ifndef LOCALMASK_H_
#define LOCALMASK_H_

#include <map>
    using std::map;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "LatLonBox.H"
#include "Mask.H"
#include "Slice.H"

class Dimension;
class Variable;



class LocalMask : public Mask
{
    public:
        LocalMask(Dimension *dim);
        virtual ~LocalMask();

        virtual int* get_data() const;
        virtual int* get_data(int64_t lo, int64_t hi);
        virtual void clear();
        virtual void adjust(const DimSlice &slice);
        virtual void adjust(const LatLonBox &box, Variable *lat, Variable *lon);
        virtual void adjust(double low, double hi, Variable *var,
                bool bitwise_or=true);
        virtual void recount();
        virtual void reindex();

    protected:
        int *data;
        int *index;
};

#endif // LOCALMASK_H_
