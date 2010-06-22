#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <iostream>
#include <string>
#include <vector>

#include <ga.h>

#include "AggregationDimension.H"
#include "AggregationVariable.H"
#include "Array.H"
#include "DataType.H"
#include "Debug.H"
#include "Dimension.H"
#include "Error.H"
#include "IndexOutOfBoundsException.H"
#include "Timing.H"
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
    TIMING("AggregationVariable::AggregationVariable(...)");
    TRACER("AggregationVariable ctor %s\n", var->get_name().c_str());
    add(var);
}


AggregationVariable::~AggregationVariable()
{
    TIMING("AggregationVariable::~AggregationVariable()");
}


void AggregationVariable::add(Variable *var)
{
    TIMING("AggregationVariable::add(Variable*)");
    TRACER("AggregationVariable::add %s\n", var->get_name().c_str());
    vars.push_back(var);
}


string AggregationVariable::get_name() const
{
    TIMING("AggregationVariable::get_name()");
    return vars[0]->get_name();
}


vector<Dimension*> AggregationVariable::get_dims() const
{
    TIMING("AggregationVariable::get_dims()");
    vector<Dimension*> dims = vars[0]->get_dims();
    dims[0] = agg_dim;
    return dims;
}


vector<Attribute*> AggregationVariable::get_atts() const
{
    TIMING("AggregationVariable::get_atts()");
    // TODO return union of attributes of all vars
    return vars[0]->get_atts();
}


DataType AggregationVariable::get_type() const
{
    TIMING("AggregationVariable::get_type()");
    return vars[0]->get_type();
}


Array* AggregationVariable::read()
{
    Array *dst = Array::create(get_type(), get_shape());
    return read(dst);
}


Array* AggregationVariable::read(Array *dst)
{
    Array *src;
    int ndim;
    vector<int64_t> dst_lo;
    vector<int64_t> dst_hi;
    vector<int64_t> src_lo;
    vector<int64_t> src_hi;

    ndim = num_dims();
    dst_lo.assign(ndim, 0);     // only first index will change, rest zeros
    src_lo.assign(ndim-1, 0);   // will not change; always zeros

    dst_hi = dst->get_shape();
    dst_hi[0] = 0;
    for (int64_t i=1,limit=dst_hi.size(); i<limit; ++i) {
        dst_hi[i] -= 1;
    }

    src = read(int64_t(0));
    src_hi = src->get_shape();
    for (int64_t i=1,limit=src_hi.size(); i<limit; ++i) {
        src_hi[i] -= 1;
    }
    dst->copy(src, src_lo, src_hi, dst_lo, dst_hi);
    for (int64_t i=1,limit=get_shape().at(0); i<limit; ++i) {
        src = read(i, src);
        dst_lo[0] += 1;
        dst_hi[0] += 1;
        dst->copy(src, src_lo, src_hi, dst_lo, dst_hi);
    }

    delete src;

    return dst;
}


Array* AggregationVariable::read(int64_t record)
{
    vector<int64_t> shape;
    Array *dst;
    
    shape = get_shape();
    shape.erase(shape.begin());
    dst = Array::create(get_type(), shape);

    return read(record, dst);
}


Array* AggregationVariable::read(int64_t record, Array *dst)
{
    int64_t index_within_var = record;

    if (record < 0 || record > get_shape().at(0)) {
        throw IndexOutOfBoundsException("AggregationVariable::read");
    }

    for (int64_t index_var=0; index_var<vars.size(); ++index_var) {
        int64_t num_records = vars.at(index_var)->get_shape().at(0);
        if (index_within_var < num_records) {
            return vars.at(index_var)->read(index_within_var, dst);
        } else {
            index_within_var -= num_records;
        }
    }

    return NULL;
}


ostream& AggregationVariable::print(ostream &os) const
{
    TIMING("AggregationVariable::print(ostream)");
    const string name(get_name());
    return os << "AggregationVariable(" << name << ")";
}
