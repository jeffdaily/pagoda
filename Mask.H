#ifndef MASK_H_
#define MASK_H_

#include <map>
    using std::map;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "LatLonBox.H"
#include "Slice.H"

class Dimension;



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
        virtual void adjust(const DimSlice &slice) = 0;
        virtual void recount() = 0;

    protected:
        Mask(const Mask &other);
        Mask& operator = (const Mask &other);

        Dimension *dim;
        bool dirty;
        long count;
};


class LocalMask : public Mask
{
    public:
        LocalMask(Dimension *dim);
        virtual ~LocalMask();

        virtual int* get_data() const;
        virtual int* get_data(int64_t lo, int64_t hi);
        virtual void adjust(const DimSlice &slice);
        virtual void recount();

    protected:
        LocalMask(const LocalMask &other);
        LocalMask& operator = (const LocalMask &other);

        int *data;
};


class DistributedMask : public Mask
{
    public:
        DistributedMask(Dimension *dim);
        virtual ~DistributedMask();

        virtual int* get_data() const;
        virtual int* get_data(int64_t lo, int64_t hi);
        virtual void adjust(const DimSlice &slice);
        virtual void recount();

    protected:
        DistributedMask(const DistributedMask &other);
        DistributedMask& operator = (const DistributedMask &other);

        int handle;
        int64_t lo;
        int64_t hi;
        long *counts;
};

#endif // MASK_H_
