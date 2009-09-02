#ifndef AGGREGATIONJOINEXISTING_H_
#define AGGREGATIONJOINEXISTING_H_

#include <iostream>
#include <string>
#include <vector>

#include "Aggregation.H"

using std::ostream;
using std::string;
using std::vector;

class AggregationDimension;
class AggregationVariable;
class Attribute;
class Dataset;
class Dimension;
class Variable;


class AggregationJoinExisting : public Aggregation
{
    public:
        AggregationJoinExisting(const string& name);
        virtual ~AggregationJoinExisting();

        virtual vector<Attribute*> get_atts();
        virtual vector<Dimension*> get_dims();
        virtual vector<Variable*> get_vars();

        virtual void add(Dataset *dataset);

        virtual ostream& print(ostream &os) const;

    protected:
        virtual void decorate_set(const vector<Variable*> &vars);

    private:
        string agg_dim_name;
        AggregationDimension *agg_dim;
        AggregationVariable *agg_var;
        vector<Dataset*> datasets;
        vector<Attribute*> atts;
        vector<Dimension*> dims;
        vector<Variable*> vars;

};

#endif // AGGREGATIONJOINEXISTING_H_
