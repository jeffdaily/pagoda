#ifndef GLOBALMASK_H_
#define GLOBALMASK_H_

#include <string>

#include "Array.H"
#include "GlobalArray.H"
#include "LatLonBox.H"
#include "Mask.H"
#include "Slice.H"

using std::string;

class Dimension;
class Variable;


class GlobalMask :  public Mask
{
    public:
        GlobalMask(Dimension *dim);
        GlobalMask(const GlobalMask &that);
        virtual ~GlobalMask();

        virtual int64_t get_size() const;
        virtual int64_t get_count() const;
        virtual void clear();
        virtual void fill();

        virtual void modify(const DimSlice &slice);
        virtual void modify(const LatLonBox &box,
                            const Array *lat, const Array *lon);
        virtual void modify(double low, double hi, const Array *var);

        virtual Array* as_array() const;
        virtual Array* reindex() const;
        virtual Array* partial_sum() const;

        virtual ostream& print(ostream &os) const;

        void normalize();

    protected:
        GlobalArray *mask;
};

#endif /* GLOBALMASK_H_ */
