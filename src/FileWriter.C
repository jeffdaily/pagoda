#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <cassert>
#include <iostream>
#include <string>
#include <vector>

#include "Dataset.H"
#include "Dimension.H"
#include "FileFormat.H"
#include "FileWriter.H"
#include "GenericAttribute.H"
#include "Mask.H"
#include "PnetcdfFileWriter.H"
#include "Util.H"
#include "Timing.H"
#include "Variable.H"

using std::ostream;
using std::string;
using std::vector;


FileWriter* FileWriter::append(const string &filename)
{
    //TIMING("FileWriter::append(string)");
    FileWriter *writer = NULL;
    string EXT_NC(".nc");
    if (pagoda::ends_with(filename, EXT_NC)) {
        writer = new PnetcdfFileWriter(filename, true);
    }
    return writer;
}


FileWriter* FileWriter::create(const string &filename)
{
    //TIMING("FileWriter::create(string)");
    FileWriter *writer = NULL;
    string EXT_NC(".nc");
    if (pagoda::ends_with(filename, EXT_NC)) {
        writer = create(filename, FF_PNETCDF_CDF2);
    }
    return writer;
}


FileWriter* FileWriter::create(const string &filename, FileFormat format)
{
    //TIMING("FileWriter::create(string)");
    FileWriter *writer = NULL;
    switch (format) {
        case FF_PNETCDF_CDF1:
        case FF_PNETCDF_CDF2:
        case FF_PNETCDF_CDF5:
            writer = new PnetcdfFileWriter(filename, format);
            break;
    }
    assert(writer);
    return writer;
}


FileWriter::FileWriter()
{
    TIMING("FileWriter::FileWriter()");
}


FileWriter::~FileWriter()
{
    TIMING("FileWriter::~FileWriter()");
}


void FileWriter::def_dataset(Dataset *dataset)
{
    write_atts(dataset->get_atts());
    def_dims(dataset->get_dims());
    def_vars(dataset->get_vars());
}


void FileWriter::def_dim(Mask *mask)
{
    def_dim(mask->get_name(), mask->get_count());
}


void FileWriter::def_dim(Dimension *dim)
{
    if (dim->is_unlimited()) {
        def_dim(dim->get_name(), 0);
    } else {
        def_dim(dim->get_name(), dim->get_size());
    }
}


void FileWriter::def_dims(const vector<Mask*> &masks)
{
    TIMING("FileWriter::def_dims(vector<Mask*>)");
    vector<Mask*>::const_iterator msk_it;
    for (msk_it=masks.begin(); msk_it!=masks.end(); ++msk_it) {
        def_dim(*msk_it);
    }
}


void FileWriter::def_dims(const vector<Dimension*> &dims)
{
    TIMING("FileWriter::def_dims(vector<Dimension*>)");
    vector<Dimension*>::const_iterator dim_it;
    for (dim_it=dims.begin(); dim_it!=dims.end(); ++dim_it) {
        def_dim(*dim_it);
    }
}


void FileWriter::def_var(const string &name, const vector<Dimension*> &dims,
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


void FileWriter::def_var(const string &name, const vector<Mask*> &masks,
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


void FileWriter::def_var(Variable *var)
{
    def_var(var->get_name(), var->get_dims(), var->get_type(), var->get_atts());
}


void FileWriter::def_vars(const vector<Variable*> &vars)
{
    TIMING("FileWriter::def_vars(vector<Variable*>)");
    vector<Variable*>::const_iterator var_it;
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        def_var(*var_it);
    }
}


void FileWriter::write_att(const string &name, Values *values,
        DataType type, const string &var_name)
{
    GenericAttribute att(name, values, type);
    write_att(&att, name);
}


void FileWriter::write_atts(const vector<Attribute*> &atts, const string &name)
{
    TIMING("FileWriter::write_atts(vector<Attribute*>,string)");
    vector<Attribute*>::const_iterator att_it;
    for (att_it=atts.begin(); att_it!=atts.end(); ++att_it) {
        write_att(*att_it, name);
    }
}


ostream& operator << (ostream &os, const FileWriter *writer)
{
    TIMING("operator<<(ostream,FileWriter*)");
    return writer->print(os);
}
