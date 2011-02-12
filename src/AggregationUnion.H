#ifndef AGGREGATIONUNION_H_
#define AGGREGATIONUNION_H_

#include <string>
#include <vector>

#include "Aggregation.H"
#include "FileFormat.H"

using std::string;
using std::vector;

class Attribute;
class Dimension;
class MaskMap;
class Variable;


/**
 * An aggregated Dataset which combines unique Dimension and Variable
 * instances by name (first found, first aggregated).
 *
 * @see
 * http://www.unidata.ucar.edu/software/netcdf/ncml/v2.2/Aggregation.html#Union
 * for details
 *
 * The aggregated Variable, Dimension, and Attribute instances are cached.
 * The methods here provide a logical view of the aggregated Dataset instances
 * but the cache does not own any of the Variable, Dimension, or Attribute
 * instances.  Do not delete them.  When the AggregationUnion destructs, it
 * will also destruct its aggregated Dataset instances.
 */
class AggregationUnion : public Aggregation
{
    public:
        /**
         * Constructs the AggregationUnion.
         */
        AggregationUnion();

        /**
         * Destructs the AggregationUnion and deletes all aggregated Dataset
         * instances.
         */
        virtual ~AggregationUnion();

        virtual void add(Dataset *dataset);
        virtual void add(const vector<Dataset*> &datasets);

        virtual ostream& print(ostream &os) const;
};

#endif // AGGREGATIONUNION_H_
