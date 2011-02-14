#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

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
#include "Variable.H"
#include "Validator.H"

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
    /** @todo return union of attributes of all vars */
    return vars[0]->get_atts();
}


bool AggregationVariable::has_validator(int64_t record) const
{
    int64_t index_within_var = translate_record(record);

    if (record < 0 || record > get_shape().at(0)) {
        throw IndexOutOfBoundsException("AggregationVariable::has_validator");
    }

    for (size_t index_var=0; index_var<vars.size(); ++index_var) {
        Variable *var = vars.at(index_var);
        int64_t num_records;
        // we want the non-masked size of this variable
        get_dataset()->push_masks(NULL);
        num_records = var->get_shape().at(0);
        get_dataset()->pop_masks();
        if (index_within_var < num_records) {
            return var->has_validator(index_within_var);
        }
        else {
            index_within_var -= num_records;
        }
    }

    return false;
}


Validator* AggregationVariable::get_validator(int64_t record) const
{
    int64_t index_within_var = translate_record(record);

    if (record < 0 || record > get_shape().at(0)) {
        throw IndexOutOfBoundsException("AggregationVariable::get_validator");
    }

    for (size_t index_var=0; index_var<vars.size(); ++index_var) {
        Variable *var = vars.at(index_var);
        int64_t num_records;
        // we want the non-masked size of this variable
        get_dataset()->push_masks(NULL);
        num_records = var->get_shape().at(0);
        get_dataset()->pop_masks();
        if (index_within_var < num_records) {
            return var->get_validator(index_within_var);
        }
        else {
            index_within_var -= num_records;
        }
    }

    return NULL;
}


Dataset* AggregationVariable::get_dataset() const
{
    return (Dataset*)agg;
}


DataType AggregationVariable::get_type() const
{
    return vars[0]->get_type();
}


Array* AggregationVariable::read(Array *dst) const
{
    if (dst == NULL) {
        return AbstractVariable::read_alloc();
    }
#if AGGREGATION_VARIABLE_READ_LOMEM
    return read_per_record(dst);
#else
    return read_per_variable(dst);
#endif
}


Array* AggregationVariable::read_per_record(Array *dst) const
{
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


Array* AggregationVariable::read_per_variable(Array *dst) const
{
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

    /**
     * @todo could optimize by reusing Array *src if shape doesn't change
     * between reads
     */
    for (var_it=vars.begin(),var_end=vars.end(); var_it!=var_end; ++var_it) {
        Variable *var = *var_it;
        Array *src = var->read();
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


Array* AggregationVariable::read(int64_t record, Array *dst) const
{
    int64_t index_within_var = translate_record(record);

    if (dst == NULL) {
        return AbstractVariable::read_alloc(record);
    }

    if (record < 0 || record > get_shape().at(0)) {
        throw IndexOutOfBoundsException("AggregationVariable::read");
    }

    for (size_t index_var=0; index_var<vars.size(); ++index_var) {
        Variable *var = vars.at(index_var);
        int64_t num_records;
        // we want the non-masked size of this variable
        get_dataset()->push_masks(NULL);
        num_records = var->get_shape().at(0);
        get_dataset()->pop_masks();
        if (index_within_var < num_records) {
            var->set_translate_record(false);
            dst = var->read(index_within_var, dst);
            var->set_translate_record(true);
            return dst;
        }
        else {
            index_within_var -= num_records;
        }
    }

    return NULL;
}


Array* AggregationVariable::iread(Array *dst)
{
    vector<Variable*>::const_iterator var_it;
    vector<Variable*>::const_iterator var_end;

    if (dst == NULL) {
        return AbstractVariable::iread_alloc();
    }

    array_to_fill = dst;

    for (var_it=vars.begin(),var_end=vars.end(); var_it!=var_end; ++var_it) {
        Variable *var = *var_it;
        arrays_to_copy.push_back(var->iread());
    }

    return dst;
}


void AggregationVariable::after_wait()
{
    int ndim = get_ndim();
    vector<int64_t> dst_lo(ndim, 0);
    vector<int64_t> dst_hi = get_shape();
    vector<int64_t>::iterator shape_it;
    vector<int64_t>::iterator shape_end;

    /* If this wasn't a full read() then we do nothing. */
    if (NULL == array_to_fill || arrays_to_copy.empty()) {
        return;
    }

    for (shape_it=dst_hi.begin(),shape_end=dst_hi.end();
            shape_it!=shape_end; ++shape_it) {
        --(*shape_it);
    }

    for (size_t i=0,limit=arrays_to_copy.size(); i<limit; ++i) {
        Array *src = arrays_to_copy[i];
        vector<int64_t> src_lo(ndim, 0);
        vector<int64_t> src_hi = src->get_shape();

        for (shape_it=src_hi.begin(),shape_end=src_hi.end();
                shape_it!=shape_end; ++shape_it) {
            --(*shape_it);
        }
        dst_hi[0] = dst_lo[0] + src_hi[0];
        array_to_fill->copy(src, src_lo, src_hi, dst_lo, dst_hi);
        dst_lo[0] = dst_hi[0]+1;
        delete src;
    }
}


Array* AggregationVariable::iread(int64_t record, Array *dst)
{
    int64_t index_within_var = translate_record(record);

    if (dst == NULL) {
        return AbstractVariable::iread_alloc(record);
    }

    if (record < 0 || record > get_shape().at(0)) {
        throw IndexOutOfBoundsException("AggregationVariable::iread");
    }

    for (size_t index_var=0; index_var<vars.size(); ++index_var) {
        Variable *var = vars.at(index_var);
        int64_t num_records;
        // we want the non-masked size of this variable
        get_dataset()->push_masks(NULL);
        num_records = var->get_shape().at(0);
        get_dataset()->pop_masks();
        if (index_within_var < num_records) {
            var->set_translate_record(false);
            dst = var->iread(index_within_var, dst);
            var->set_translate_record(true);
            return dst;
        }
        else {
            index_within_var -= num_records;
        }
    }

    return NULL;
}


ostream& AggregationVariable::print(ostream &os) const
{
    const string name(get_name());
    return os << "AggregationVariable(" << name << ")";
}
