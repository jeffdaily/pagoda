#include <iostream>
#include <string>
#include <vector>

#include "AggregationVariable.H"
#include "Common.H"
#include "DataType.H"
#include "Dimension.H"
#include "Variable.H"

using std::ostream;
using std::string;
using std::vector;


AggregationVariable::AggregationVariable(Dimension *dim, Variable *var)
    :   AbstractVariable()
    ,   index_var(0)
    ,   index_within_var(0)
    ,   dim(dim)
    ,   vars()
{
    add(var);
}


AggregationVariable::~AggregationVariable()
{
}


void AggregationVariable::add(Variable *var)
{
    vars.push_back(var);
}


string AggregationVariable::get_name() const
{
    return vars[0]->get_name();
}


vector<Dimension*> AggregationVariable::get_dims() const
{
    vector<Dimension*> dims = vars[0]->get_dims();
    dims[0] = dim;
    return dims;
}


vector<Attribute*> AggregationVariable::get_atts() const
{
    // TODO return union of attributes of all vars
    return vars[0]->get_atts();
}


DataType AggregationVariable::get_type() const
{
    return vars[0]->get_type();
}


int AggregationVariable::get_handle()
{
    if (has_record()) {
        return vars[index_var]->get_handle();
    } else {
        return AbstractVariable::get_handle();
    }
}


void AggregationVariable::release_handle()
{
    AggregationVariable::release_handle();
    for (size_t varidx=0; varidx<vars.size(); ++varidx) {
        vars[varidx]->release_handle();
    }
}


void AggregationVariable::set_record_index(size_t index)
{
    AbstractVariable::set_record_index(index);

    index_within_var = index;
    for (index_var=0; index_var<vars.size(); ++index_var) {
        int64_t size = vars[index_var]->get_dims()[0]->get_size();
        if (index < size) {
            break;
        } else {
            index_within_var -= size;
        }
    }
    if (index_var == vars.size()) {
        throw "AggregationVariable: index_var == vars.size()";
    }
    vars[index_var]->set_record_index(index_within_var);
}


void AggregationVariable::read()
{
    if (has_record()) {
        vars[index_var]->read();
    } else {
        // TODO read each var and use NGA_Copy_patch to copy portions into
        // this AggregationVariable's larger array
        throw "TODO";
        /*
        int myhandle = get_handle();
        int64_t pos = 0;
        for (size_t varidx=0; varidx<vars.size(); ++varidx) {
            //vars[varidx]->read();
        }
        */
    }
}


ostream& AggregationVariable::print(ostream &os) const
{
    const string name(get_name());
    return os << "AggregationVariable(" << name << ")";
}
