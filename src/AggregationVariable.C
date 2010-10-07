#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <iostream>
#include <string>
#include <vector>

#include "Aggregation.H"
#include "AggregationDimension.H"
#include "AggregationVariable.H"
#include "Array.H"
#include "Dataset.H"
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
        Aggregation *agg,
        AggregationDimension *agg_dim,
        Variable *var)
    :   AbstractVariable()
    ,   agg(agg)
    ,   agg_dim(agg_dim)
    ,   vars()
    ,   arrays_to_copy()
    ,   array_to_fill(NULL)
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


Dataset* AggregationVariable::get_dataset() const
{
    TIMING("AggregationVariable::get_dataset()");
    return (Dataset*)agg;
}


DataType AggregationVariable::get_type() const
{
    TIMING("AggregationVariable::get_type()");
    return vars[0]->get_type();
}


#if AGGREGATION_VARIABLE_READ_LOMEM
Array* AggregationVariable::read(Array *dst) const
{
    /* Reads one timestep at a time and copies to dst Array */
    Array *src;
    int ndim;
    vector<int64_t> dst_lo;
    vector<int64_t> dst_hi;
    vector<int64_t> src_lo;
    vector<int64_t> src_hi;

    ndim = get_ndim();
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

#else /* AGGREGATION_VARIABLE_READ_LOMEM */

Array* AggregationVariable::read(Array *dst) const
{    /* Reads one aggregated Variable at a time and copies to dst Array */
    int count = 0;
    int ndim = get_ndim();
    vector<int64_t> dst_lo(ndim, 0);
    vector<int64_t> dst_hi = get_shape();
    vector<int64_t>::iterator shape_it;
    vector<int64_t>::iterator shape_end;
    vector<Variable*>::const_iterator var_it;
    vector<Variable*>::const_iterator var_end;

    for (shape_it=dst_hi.begin(),shape_end=dst_hi.end();
            shape_it!=shape_end; ++shape_it) {
        --(*shape_it);
    }
    dst_hi[0] = 0;

    // TODO could optimize by reusing Array *src if shape doesn't change
    // between reads
    for (var_it=vars.begin(),var_end=vars.end(); var_it!=var_end; ++var_it) {
        Variable *var = *var_it;
        Array *src = var->read();
        int64_t ndim = var->get_ndim();
        vector<int64_t> src_lo(ndim, 0);
        vector<int64_t> src_hi = var->get_shape();

        for (shape_it=src_hi.begin(),shape_end=src_hi.end();
                shape_it!=shape_end; ++shape_it) {
            --(*shape_it);
        }
        dst_hi[0] = dst_lo[0] + src_hi[0];
        dst->copy(src, src_lo, src_hi, dst_lo, dst_hi);
        dst_lo[0] = dst_hi[0]+1;
        delete src;
    }

    return dst;
}
#endif  /* AGGREGATION_VARIABLE_READ_LOMEM */


Array* AggregationVariable::read(int64_t record, Array *dst) const
{
    int64_t index_within_var = translate_record(record);

    if (record < 0 || record > get_shape().at(0)) {
        throw IndexOutOfBoundsException("AggregationVariable::read");
    }

    for (int64_t index_var=0; index_var<vars.size(); ++index_var) {
        Variable *var = vars.at(index_var);
        int64_t num_records;
        // we want the non-masked size of this variable
        get_dataset()->push_masks(NULL);
        num_records = var->get_shape().at(0);
        get_dataset()->pop_masks();
        if (index_within_var < num_records) {
            return vars.at(index_var)->read(index_within_var, dst);
        } else {
            index_within_var -= num_records;
        }
    }

    return NULL;
}


Array* AggregationVariable::iread(Array *dst)
{
    vector<Variable*>::const_iterator var_it;
    vector<Variable*>::const_iterator var_end;

    array_to_fill = dst;
    
    for (var_it=vars.begin(),var_end=vars.end(); var_it!=var_end; ++var_it) {
        Variable *var = *var_it;
        arrays_to_copy.push_back(var->iread());
    }

    return dst;
}


Array* AggregationVariable::iread(int64_t record, Array *dst)
{
    int64_t index_within_var = record;

    if (record < 0 || record > get_shape().at(0)) {
        throw IndexOutOfBoundsException("AggregationVariable::read");
    }

    for (int64_t index_var=0; index_var<vars.size(); ++index_var) {
        int64_t num_records = vars.at(index_var)->get_shape().at(0);
        if (index_within_var < num_records) {
            return vars.at(index_var)->iread(index_within_var, dst);
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
