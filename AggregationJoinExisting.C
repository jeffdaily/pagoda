#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <iostream>
#include <string>
#include <vector>

#include "AggregationDimension.H"
#include "AggregationJoinExisting.H"
#include "AggregationVariable.H"
#include "Attribute.H"
#include "Debug.H"
#include "Dimension.H"
#include "Variable.H"

using std::ostream;
using std::string;
using std::vector;


AggregationJoinExisting::AggregationJoinExisting(const string& name)
    :   Aggregation()
    ,   agg_dim_name(name)
    ,   agg_dim(NULL)
    ,   agg_var(NULL)
    ,   datasets()
    ,   atts()
    ,   dims()
    ,   vars()
{
    TRACER1("AggregationJoinExisting::ctor(%s)\n", name.c_str());
}


AggregationJoinExisting::~AggregationJoinExisting()
{
    delete agg_dim;
    delete agg_var;
}


vector<Attribute*> AggregationJoinExisting::get_atts()
{
    return atts;
}


vector<Dimension*> AggregationJoinExisting::get_dims()
{
    return dims;
}


vector<Variable*> AggregationJoinExisting::get_vars()
{
    return vars;
}


void AggregationJoinExisting::add(Dataset *dataset)
{
    TRACER("AggregationJoinExisting::add BEGIN\n");
    datasets.push_back(dataset);

    vector<Attribute*> other_atts = dataset->get_atts();
    vector<Attribute*>::const_iterator other_atts_it = other_atts.begin();
    vector<Attribute*>::const_iterator other_atts_end = other_atts.end();
    for (; other_atts_it!=other_atts_end; ++other_atts_it) {
        Attribute *other_att = *other_atts_it;
        Attribute *att = find_att(other_att->get_name());
        if (! att) {
            atts.push_back(other_att);
        }
    }

    vector<Dimension*> other_dims = dataset->get_dims();
    vector<Dimension*>::const_iterator other_dims_it = other_dims.begin();
    vector<Dimension*>::const_iterator other_dims_end = other_dims.end();
    for (; other_dims_it!=other_dims_end; ++other_dims_it) {
        Dimension *other_dim = *other_dims_it;
        Dimension *dim = find_dim(other_dim->get_name());
        if (! dim) {
            if (other_dim->get_name() == agg_dim_name) {
                agg_dim = new AggregationDimension(other_dim);
                dims.push_back(agg_dim);
            } else {
                dims.push_back(other_dim);
            }
        } else if (other_dim->get_name() == agg_dim_name) {
            agg_dim->add(other_dim);
        }
    }

    vector<Variable*> other_vars = dataset->get_vars();
    vector<Variable*>::const_iterator other_vars_it = other_vars.begin();
    vector<Variable*>::const_iterator other_vars_end = other_vars.end();
    for (; other_vars_it!=other_vars_end; ++other_vars_it) {
        Variable *other_var = *other_vars_it;
        Variable *var = find_var(other_var->get_name());
        if (! var) {
            if (other_var->get_dims()[0]->get_name() == agg_dim_name) {
                agg_var = new AggregationVariable(agg_dim, other_var);
                vars.push_back(agg_var);
            } else {
                vars.push_back(other_var);
            }
        } else if (other_var->get_dims()[0]->get_name() == agg_dim_name) {
            agg_var->add(other_var);
        }
    }
    TRACER("AggregationJoinExisting::add END\n");
}


ostream& AggregationJoinExisting::print(ostream &os) const
{
    return os << "AggregationJoinExisting()";
}


void AggregationJoinExisting::decorate_set(const vector<Variable*> &vars)
{
    this->vars = vars;
}
