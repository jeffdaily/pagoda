#ifndef MASK_H_
#define MASK_H_

#include <string>

#include "LatLonBox.H"
#include "Slice.H"

using std::string;

class Dimension;
class Variable;


class Mask
{
    public:
        Mask(Dimension *dim);
        virtual ~Mask();

        string get_name() const;
        Dimension* get_dim() const;
        int64_t get_size() const;
        int64_t get_count();

        virtual void get_data(vector<int> &data) = 0;
        virtual void get_data(vector<int> &data, int64_t lo, int64_t hi) = 0;
        virtual void clear() = 0;
        virtual void adjust(const DimSlice &slice) = 0;
        virtual void adjust(const LatLonBox &box,
                Variable *lat, Variable *lon) = 0;
        virtual void adjust(double low, double hi, Variable *var,
                bool bitwise_or=true) = 0;
        virtual void recount() = 0;
        virtual void reindex() = 0;

    protected:
        Dimension *dim;
        bool need_recount;
        bool cleared;
        int64_t count;
};

#endif // MASK_H_
