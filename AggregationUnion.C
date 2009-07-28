#include <algorithm>
    using std::find_if;
    using std::transform;
#include <functional>
    using std::bind2nd;
    using std::ptr_fun;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "AggregationUnion.H"
#include "Attribute.H"
#include "Dimension.H"
#include "Util.H"
    using Util::ptr_deleter;
    using Util::same_name;
#include "Variable.H"


AggregationUnion::AggregationUnion()
    :   Aggregation()
    ,   datasets()
    ,   atts()
    ,   dims()
    ,   vars()
{
}


AggregationUnion::~AggregationUnion()
{
    // deleting the datasets should also delete their associate members
    transform(datasets.begin(), datasets.end(),
            datasets.begin(), ptr_deleter<Dataset*>);
}


vector<Attribute*> AggregationUnion::get_atts()
{
    return atts;
}


vector<Dimension*> AggregationUnion::get_dims()
{
    return dims;
}


vector<Variable*> AggregationUnion::get_vars()
{
    return vars;
}


void AggregationUnion::add(Dataset *dataset)
{
    datasets.push_back(dataset);

    vector<Attribute*> other_atts = dataset->get_atts();
    for (size_t i=0,limit=other_atts.size(); i<limit; ++i) {
        Attribute *att = other_atts[i];
        vector<Attribute*>::iterator iter;
        iter = find_if(atts.begin(), atts.end(),
                bind2nd(ptr_fun(same_name<Attribute>), att));
        if (iter == atts.end()) {
            atts.push_back(att);
        }
    }

    vector<Dimension*> other_dims = dataset->get_dims();
    for (size_t i=0,limit=other_dims.size(); i<limit; ++i) {
        Dimension *dim = other_dims[i];
        vector<Dimension*>::iterator iter;
        iter = find_if(dims.begin(), dims.end(),
                bind2nd(ptr_fun(same_name<Dimension>), dim));
        if (iter == dims.end()) {
            dims.push_back(dim);
        }
    }

    vector<Variable*> other_vars = dataset->get_vars();
    for (size_t i=0,limit=other_vars.size(); i<limit; ++i) {
        Variable *var = other_vars[i];
        vector<Variable*>::iterator iter;
        iter = find_if(vars.begin(), vars.end(),
                bind2nd(ptr_fun(same_name<Variable>), var));
        if (iter == vars.end()) {
            vars.push_back(var);
        }
    }
}


void AggregationUnion::add(const vector<Dataset*> &datasets)
{
    vector<Dataset*>::const_iterator it;
    for (it=datasets.begin(); it!=datasets.end(); ++it) {
        add(*it);
    }
}


ostream& AggregationUnion::print(ostream &os) const
{
    return os << "AggregationUnion()";
}


void AggregationUnion::decorate(const vector<Variable*> &vars)
{
    this->vars = vars;
}

