#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <string>
#include <vector>

#include "AggregationUnion.H"
#include "Attribute.H"
#include "Dimension.H"
#include "Error.H"
#include "Timing.H"
#include "Util.H"
#include "Variable.H"

using std::string;
using std::transform;
using std::vector;


AggregationUnion::AggregationUnion()
    :   Aggregation()
    ,   datasets()
    ,   atts()
    ,   dims()
    ,   vars()
{
    TIMING("AggregationUnion::AggregationUnion()");
}


AggregationUnion::~AggregationUnion()
{
    // deleting the datasets should also delete their associate members
    // and close any open files
    TIMING("AggregationUnion::~AggregationUnion()");

    transform(datasets.begin(), datasets.end(), datasets.begin(),
            pagoda::ptr_deleter<Dataset*>);
}


vector<Attribute*> AggregationUnion::get_atts() const
{
    TIMING("AggregationUnion::get_atts()");
    return atts;
}


vector<Dimension*> AggregationUnion::get_dims() const
{
    TIMING("AggregationUnion::get_dims()");
    return dims;
}


vector<Variable*> AggregationUnion::get_vars() const
{
    TIMING("AggregationUnion::get_vars()");
    return vars;
}


void AggregationUnion::add(Dataset *dataset)
{
    TIMING("AggregationUnion::add(Dataset*)");
    datasets.push_back(dataset);

    vector<Attribute*> other_atts = dataset->get_atts();
    vector<Attribute*>::const_iterator other_atts_it = other_atts.begin();
    vector<Attribute*>::const_iterator other_atts_end = other_atts.end();
    for (; other_atts_it!=other_atts_end; ++other_atts_it) {
        Attribute *other_att = *other_atts_it;
        Attribute *orig_att = get_att(other_att->get_name());
        if (! orig_att) {
            atts.push_back(other_att);
        }
    }

    vector<Dimension*> other_dims = dataset->get_dims();
    vector<Dimension*>::const_iterator other_dims_it = other_dims.begin();
    vector<Dimension*>::const_iterator other_dims_end = other_dims.end();
    for (; other_dims_it!=other_dims_end; ++other_dims_it) {
        Dimension *other_dim = *other_dims_it;
        Dimension *orig_dim = get_dim(other_dim->get_name());
        if (! orig_dim) {
            dims.push_back(other_dim);
        } else {
            // dim already exists, but make sure the sizes are the same
            if (orig_dim->get_size() != other_dim->get_size()) {
                ERR("union dimension mismatch");
            }
        }
    }

    vector<Variable*> other_vars = dataset->get_vars();
    vector<Variable*>::const_iterator other_vars_it = other_vars.begin();
    vector<Variable*>::const_iterator other_vars_end = other_vars.end();
    for (; other_vars_it!=other_vars_end; ++other_vars_it) {
        Variable *other_var = *other_vars_it;
        Variable *orig_var = get_var(other_var->get_name());
        if (! orig_var) {
            vars.push_back(other_var);
        }
    }
}


vector<Dataset*> AggregationUnion::get_datasets() const
{
    return datasets;
}


void AggregationUnion::wait()
{
    for (vector<Dataset*>::iterator it=datasets.begin(), end=datasets.end();
            it!=end; ++it) {
        (*it)->wait();
    }
}


ostream& AggregationUnion::print(ostream &os) const
{
    TIMING("AggregationUnion::print(ostream)");
    return os << "AggregationUnion()";
}
