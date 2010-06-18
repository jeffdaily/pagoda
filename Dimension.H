#ifndef DIMENSION_H_
#define DIMENSION_H_

#include <iostream>
#include <string>

using std::ostream;
using std::string;


/**
 * A Dimension is a named size.
 *
 * Although it is illogical for a Dimension's size to be less than or equal to
 * zero, this interface does allow it due to the way certain libraries
 * implement Dimensions (notably the netcdf library) as integers.
 *
 * An unlimited Dimension is also taken into account -- another throwback to
 * netcdf which this interface is modeled after.
 */
class Dimension
{
    public:
        /**
         * Dimension destructor.
         */
        virtual ~Dimension();

        /**
         * Returns the name of this Dimension.
         *
         * @return the name of this Dimension.
         */
        virtual string get_name() const = 0;

        /**
         * Returns the size of this Dimension.
         *
         * @return the size of this Dimension.
         */
        virtual int64_t get_size() const = 0;

        /**
         * Returns true if this Dimension is of unlimited size.
         *
         * @return true if this Dimension is of unlimited size.
         */
        virtual bool is_unlimited() const = 0;

        /**
         * Write this Dimension to the given stream.
         * 
         * @param[in,out] os the stream
         * @return the stream
         */
        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Dimension *other);

    protected:
        /**
         * Protected constructor.
         */
        Dimension();

        /**
         * Protected copy constructor.
         *
         * @param[in] copy the Dimension to copy
         */
        Dimension(const Dimension &copy);

        /**
         * Protected assignment operator.
         *
         * @param[in] copy the Dimension to copy
         */
        Dimension& operator = (const Dimension &copy);
};

#endif // DIMENSION_H_
