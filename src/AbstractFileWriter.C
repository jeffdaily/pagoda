#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <cassert>
#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "AbstractFileWriter.H"
#include "Dataset.H"
#include "Dimension.H"
#include "FileFormat.H"
#include "GenericAttribute.H"
#include "Mask.H"
#include "Util.H"
#include "Variable.H"

using std::ostream;
using std::set;
using std::string;
using std::vector;


AbstractFileWriter::AbstractFileWriter()
{
}


AbstractFileWriter::~AbstractFileWriter()
{
}


void AbstractFileWriter::def_dataset(Dataset *dataset)
{
    write_atts(dataset->get_atts());
    def_dims(dataset->get_dims());
    def_vars(dataset->get_vars());
}


void AbstractFileWriter::def_dim(Mask *mask)
{
    def_dim(mask->get_name(), mask->get_count());
}


void AbstractFileWriter::def_dim(Dimension *dim)
{
    if (dim->is_unlimited()) {
        def_dim(dim->get_name(), 0);
    }
    else {
        def_dim(dim->get_name(), dim->get_size());
    }
}


void AbstractFileWriter::def_dims(const vector<Mask*> &masks)
{
    vector<Mask*>::const_iterator msk_it;
    for (msk_it=masks.begin(); msk_it!=masks.end(); ++msk_it) {
        def_dim(*msk_it);
    }
}


void AbstractFileWriter::def_dims(const vector<Dimension*> &dims)
{
    vector<Dimension*>::const_iterator dim_it;
    for (dim_it=dims.begin(); dim_it!=dims.end(); ++dim_it) {
        def_dim(*dim_it);
    }
}


void AbstractFileWriter::def_var(const string &name, const vector<Dimension*> &dims,
                         const DataType &type, const vector<Attribute*> &atts)
{
    vector<Dimension*>::const_iterator dim_it;
    vector<Dimension*>::const_iterator dim_end;
    vector<string> dim_names;

    for (dim_it=dims.begin(),dim_end=dims.end(); dim_it!=dim_end; ++dim_it) {
        dim_names.push_back((*dim_it)->get_name());
    }

    def_var(name, dim_names, type, atts);
}


void AbstractFileWriter::def_var(const string &name, const vector<Mask*> &masks,
                         const DataType &type, const vector<Attribute*> &atts)
{
    vector<Mask*>::const_iterator msk_it;
    vector<Mask*>::const_iterator msk_end;
    vector<string> dim_names;

    for (msk_it=masks.begin(),msk_end=masks.end(); msk_it!=msk_end; ++msk_it) {
        dim_names.push_back((*msk_it)->get_name());
    }

    def_var(name, dim_names, type, atts);
}


void AbstractFileWriter::def_var(Variable *var)
{
    def_var(var->get_name(), var->get_dims(), var->get_type(), var->get_atts());
}


void AbstractFileWriter::def_vars(const vector<Variable*> &vars)
{
    vector<Variable*>::const_iterator var_it;
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        def_var(*var_it);
    }
}


void AbstractFileWriter::write_att(const string &name, Values *values,
                           DataType type, const string &var_name)
{
    GenericAttribute att(name, values, type);
    write_att(&att, var_name);
}


void AbstractFileWriter::write_atts(const vector<Attribute*> &atts, const string &name)
{
    vector<Attribute*>::const_iterator att_it;
    for (att_it=atts.begin(); att_it!=atts.end(); ++att_it) {
        write_att(*att_it, name);
    }
}


void AbstractFileWriter::copy_vars(const vector<Variable*> &vars)
{
    for (vector<Variable*>::const_iterator it=vars.begin(), end=vars.end();
            it!=end; ++it) {
        Variable *var = *it;
        Array *array = var->read();
        write(array, var->get_name());
        delete array;
    }
}


void AbstractFileWriter::icopy_vars(const vector<Variable*> &vars)
{
    set<Dataset*> datasets;
    vector<Array*> arrays;
    vector<string> names;
    for (vector<Variable*>::const_iterator it=vars.begin(), end=vars.end();
            it!=end; ++it) {
        Variable *var = *it;
        arrays.push_back(var->iread());
        names.push_back(var->get_name());
        datasets.insert(var->get_dataset());
    }
    for (set<Dataset*>::iterator it=datasets.begin(), end=datasets.end();
            it!=end; ++it) {
        (*it)->wait();
    }
    for (size_t i=0,limit=arrays.size(); i<limit; ++i) {
        iwrite(arrays[i], names[i]);
    }
    wait();
    for (size_t i=0,limit=arrays.size(); i<limit; ++i) {
        delete arrays[i];
    }
}


ostream& operator << (ostream &os, const AbstractFileWriter *writer)
{
    return writer->print(os);
}
