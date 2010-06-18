#ifndef AGGREGATIONJOINEXISTING_H_
#define AGGREGATIONJOINEXISTING_H_

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "Aggregation.H"

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
class Variable;


/**
 * An aggregated Dataset which joins on existing Dimensions.
 */
class AggregationJoinExisting : public Aggregation
{
    public:
        AggregationJoinExisting(const string& name);
        virtual ~AggregationJoinExisting();

        virtual vector<Attribute*> get_atts();
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Variable*> get_vars();

        virtual void add(Dataset *dataset);

        virtual Array* read();
        virtual Array* read(Array *dst);
        virtual Array* read(int64_t record);
        virtual Array* read(int64_t record, Array *dst);

        virtual ostream& print(ostream &os) const;

    private:
        string agg_dim_name;
        AggregationDimension *agg_dim;
        map<string,AggregationVariable*> agg_vars;
        vector<Dataset*> datasets;
        vector<Attribute*> atts;
        vector<Dimension*> dims;
        vector<Variable*> vars;

};

#endif // AGGREGATIONJOINEXISTING_H_
