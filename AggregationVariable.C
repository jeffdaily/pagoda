#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <iostream>
#include <string>
#include <vector>

#include <ga.h>

#include "AggregationDimension.H"
#include "AggregationVariable.H"
#include "DataType.H"
#include "Debug.H"
#include "Dimension.H"
#include "Error.H"
#include "Variable.H"

using std::ostream;
using std::string;
using std::vector;


AggregationVariable::AggregationVariable(
        AggregationDimension *agg_dim, Variable *var)
    :   AbstractVariable()
    ,   index_var(0)
    ,   index_within_var(0)
    ,   agg_dim(agg_dim)
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
    dims[0] = agg_dim;
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
    if (has_record() && num_dims() > 1) {
        return vars[index_var]->get_handle();
    } else {
        return AbstractVariable::get_handle();
    }
}


void AggregationVariable::release_handle()
{
    TRACER("AggregationVariable::release_handle() BEGIN\n");
    AggregationVariable::release_handle();
    for (size_t varidx=0; varidx<vars.size(); ++varidx) {
        vars[varidx]->release_handle();
    }
    TRACER("AggregationVariable::release_handle() END\n");
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
        ERR("AggregationVariable: index_var == vars.size()");
    }
    vars[index_var]->set_record_index(index_within_var);
}


void AggregationVariable::read()
{
    TRACER("AggregationVariable::read() BEGIN\n");
    if (has_record() && num_dims() > 1) {
        vars[index_var]->read();
    } else {
        int ndim = num_dims();
        vector<int64_t> src_lo(ndim, 0);
        vector<int64_t> src_hi(ndim, 0);
        vector<int64_t> dst_lo(ndim, 0);
        vector<int64_t> dst_hi(ndim, 0);
        vector<Variable*>::iterator var_it = vars.begin();
        vector<Variable*>::iterator var_end = vars.end();
        for (; var_it!=var_end; ++var_it) {
            Variable *src = *var_it;
            // prep for the copy
            src->read();
            src_hi = src->get_sizes();
            for (size_t i=0; i<src_hi.size(); ++i) {
                src_hi[i] -= 1;
            }
            dst_hi[0] += src_hi[0];
            // do the copy
            TRACER4("NGA_Copy_patch64 src_lo,hi=%ld,%ld, dst_lo,hi=%ld,%ld\n",
                    src_lo[0], src_hi[0], dst_lo[0], dst_hi[0]);
            NGA_Copy_patch64('n', src->get_handle(), &src_lo[0], &src_hi[0],
                    get_handle(), &dst_lo[0], &dst_hi[0]);
            // prepare for next iteration
            dst_hi[0] += 1;
            dst_lo[0] = dst_hi[0];
            src->release_handle();
        }
    }
    TRACER("AggregationVariable::read() END\n");
}


ostream& AggregationVariable::print(ostream &os) const
{
    const string name(get_name());
    return os << "AggregationVariable(" << name << ")";
}
