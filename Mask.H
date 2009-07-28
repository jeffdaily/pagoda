#ifndef MASK_H_
#define MASK_H_

#include <string>
    using std::string;

#include "LatLonBox.H"
#include "Slice.H"

class Dimension;
class Variable;


class Mask
{
    public:
        Mask(Dimension *dim);
        virtual ~Mask();

        string get_name() const;
        Dimension* get_dim() const;
        long get_count();

        virtual int* get_data() const = 0;
        virtual int* get_data(int64_t lo, int64_t hi) = 0;
        virtual void clear() = 0;
        virtual void adjust(const DimSlice &slice) = 0;
        virtual void adjust(const LatLonBox &box,
                Variable *lat, Variable *lon) = 0;
        virtual void adjust(double low, double hi, Variable *var,
                bool bitwise_or=true) = 0;
        virtual void recount() = 0;
        virtual void reindex() = 0;

        virtual bool was_cleared() { return cleared; }

    protected:
        Dimension *dim;
        bool dirty;
        bool cleared;
        long count;
};

#endif // MASK_H_
