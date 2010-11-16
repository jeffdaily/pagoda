#ifndef AGGREGATIONJOINEXISTING_H_
#define AGGREGATIONJOINEXISTING_H_

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "Aggregation.H"
#include "FileFormat.H"

using std::ostream;
using std::map;
using std::string;
using std::vector;

class AggregationDimension;
class AggregationVariable;
class Array;
class Attribute;
class Dataset;
class Dimension;
class MaskMap;
class Variable;


/**
 * An aggregated Dataset which joins on an existing Dimension instance.
 *
 * @see
 * http://www.unidata.ucar.edu/software/netcdf/ncml/v2.2/Aggregation.html#joinExisting
 * for details.
 *
 * The aggregated Variable, Dimension, and Attribute instances are cached.
 * The methods here provide a logical view of the aggregated Dataset instances
 * but the cache does not own any of the Variable, Dimension, or Attribute
 * instances.  Do not delete them.  When the AggregationUnion destructs, it
 * will also destruct its aggregated Dataset instances.
 */
class AggregationJoinExisting : public Aggregation
{
    public:
        AggregationJoinExisting(const string& name);
        virtual ~AggregationJoinExisting();

        virtual vector<Attribute*> get_atts() const;
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Variable*> get_vars() const;

        virtual void add(Dataset *dataset);
        virtual void add(const vector<Dataset*> &datasets);
        virtual vector<Dataset*> get_datasets() const;

        /**
         * @copybrief Dataset::wait()
         * @copydetails Dataset::wait()
         *
         * Sequentially waits on all aggregated Dataset instances.
         */
        virtual void wait();

        virtual FileFormat get_file_format() const;

        virtual ostream& print(ostream &os) const;

    private:
        string agg_dim_name; /**< name of Dimension joining over */
        AggregationDimension *agg_dim; /**< joined Dimension */
        map<string,AggregationVariable*> agg_vars; /**< cache of joined
Variable instances i.e. Variable instances which have the joined Dimension as
their outer-most Dimension */
        vector<Dataset*> datasets; /**< aggregated Dataset instances */
        vector<Attribute*> atts; /**< aggregated Attribute instances */
        vector<Dimension*> dims; /**< aggregated Dimension instances; this also
contains the AggregationDimension instance */
        vector<Variable*> vars; /**< aggregated Variable instances; this also
contains any AggregationVariable instances */
};

#endif // AGGREGATIONJOINEXISTING_H_
