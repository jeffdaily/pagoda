#ifndef MASK_H_
#define MASK_H_

#include <stdint.h>

#include <iostream>
#include <string>

#include "AbstractArray.H"
#include "IndexHyperslab.H"
#include "LatLonBox.H"

using std::ostream;
using std::string;

class Dimension;


/**
 * Dimension Mask interface.
 *
 * A Mask is conceptually a 1-dimensional bitmask.  However, in this case a
 * Mask is distributed just like the Array (a Mask is an Array).  Further, a
 * Mask is assumed to be a 1-dimensional integer Array.  Set bits are any
 * value not equal to 0.  Unset bits are always 0.
 *
 * Masks are used to indicate which portions of a Dimension are viable during
 * operations, such as subsetting, etc.
 */
class Mask : public AbstractArray
{
    public:
        /**
         * Factory Mask creation.
         *
         * @param[in] name the name of the Mask
         * @param[in] size the size of the Mask
         */
        static Mask* create(const string &name, int64_t size);

        /**
         * Factory Mask creation convenience function.
         *
         * The returned Mask will have the same number of elements as the
         * given Dimension.
         *
         * @param[in] dim the Dimension to mask
         */
        static Mask* create(const Dimension *dim);

        /**
         * Factory Mask creation for N-dimensional masks.
         *
         * @param[in] shape of the mask
         */
        static Mask* create(const vector<int64_t> &shape);

        /**
         * Mask destructor.
         */
        virtual ~Mask();

        /**
         * Returns the name of the associated Dimension.
         *
         * @return the name of the associated Dimension
         */
        virtual string get_name() const = 0;

        /**
         * Returns the number of set bits in this Mask.
         *
         * @return the number of set bits in this Mask
         */
        virtual int64_t get_count() = 0;

        /**
         * Set all mask bits to zero.
         */
        virtual void clear() = 0;

        /**
         * Set all mask bits to one.
         */
        virtual void reset() = 0;

        /**
         * Set bits to one if they fall within the given hyperslab.
         *
         * @param[in] hyperslab the specified hyperslab to mask
         */
        virtual void modify(const IndexHyperslab &hyperslab) = 0;

        /**
         * Set bits to one if they fall within the given LatLonBox.
         *
         * This version of modify assumes the lat and lon Arrays share a
         * single Dimension.
         *
         * @param[in] box the latitude/longitude box
         * @param[in] lat the latitude coordinate Array
         * @param[in] lon the longitude coordinate Array
         */
        virtual void modify(const LatLonBox &box,
                            const Array *lat, const Array *lon) = 0;

        /**
         * Set bits to one if they fall within the given low and hi range.
         *
         * Assumes the given Array has the same size as this Mask.
         * In other words, the given Array is a coordinate Array.
         *
         * @param[in] low the lower end of the range
         * @param[in] hi the upper end of the range
         * @param[in] var the values to compare against low and hi
         */
        virtual void modify(double low, double hi, const Array *var) = 0;

        /**
         * Set bits to one if they fall within the half-open range [value,..].
         *
         * Assumes the given Array has the same size as this Mask.
         * In other words, the given Array is a coordinate Array.
         *
         * @param[in] value values must be greater than this value
         * @param[in] var the values to compare against low and hi
         */
        virtual void modify_gt(double value, const Array *var) = 0;

        /**
         * Set bits to one if they fall within the half-open range [..,value].
         *
         * Assumes the given Array has the same size as this Mask.
         * In other words, the given Array is a coordinate Array.
         *
         * @param[in] value values must be less than this value
         * @param[in] var the values to compare against low and hi
         */
        virtual void modify_lt(double value, const Array *var) = 0;

        /**
         * Return a new Array containing the enumeration of this Mask.
         *
         * The enumeration takes into account the masked bits.  The first set
         * bit is 0, the next is 1, and so on.  Any unset bit is set to -1.
         *
         * The Array should NOT be deleted -- it is owned by this Mask.  The
         * enumeration will be recomputed only as needed, as well.
         *
         * For example:
         * Mask:   0  0  0  0  1  1  1  0  1  2 -1  0  1
         * Rtrn:  -1 -1 -1 -1  0  1  2 -1  3  4  5 -1  6
         *
         * @return the enumerated, reindexed Array
         */
        virtual Array* reindex() = 0;

        /**
         * Return a new Array containing the partial sum of this Mask.
         *
         * The Array should NOT be deleted -- it is owned by this Mask.  The
         * partial_sum will be recomputed only as needed, as well.
         *
         * @param[in] exclude whether the first value is always 0
         * @return the partially summed Array
         */
        virtual Array* partial_sum(bool exclude) = 0;

        /**
         * Write this Mask to the given stream.
         *
         * @param[in,out] os the stream
         * @return the stream
         */
        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Mask *mask);

    protected:
        /**
         * Protected default constructor.
         */
        Mask();
};

#endif // MASK_H_
