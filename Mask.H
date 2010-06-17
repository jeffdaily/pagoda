#ifndef MASK_H_
#define MASK_H_

#include <iostream>
#include <string>

#include "Array.H"
#include "LatLonBox.H"
#include "Slice.H"

using std::ostream;
using std::string;

class Dimension;


class Mask : public Array
{
    public:
        static Mask* create(const Dimension *dim);

        virtual ~Mask();

        virtual int64_t get_count() const = 0;
        virtual void clear() = 0;
        virtual void reset() = 0;

        virtual void modify(const DimSlice &slice) = 0;
        virtual void modify(const LatLonBox &box,
                            const Array *lat, const Array *lon) = 0;
        virtual void modify(double low, double hi, const Array *var) = 0;

        virtual Array* reindex() const = 0;
        virtual Array* partial_sum(bool exclude) const = 0;

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Mask *mask);

    protected:
        Mask();
};

#endif // MASK_H_
