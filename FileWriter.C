#include <iostream>
#include <string>
#include <vector>

#include "Common.H"
#include "FileWriter.H"
#include "NetcdfFileWriter.H"
#include "Util.H"

using std::ostream;
using std::string;
using std::vector;


FileWriter* FileWriter::create(const string &filename)
{
    FileWriter *writer = NULL;
    string EXT_NC(".nc");
    if (Util::ends_with(filename, EXT_NC)) {
        writer = new NetcdfFileWriter(filename);
    }
    return writer;
}


FileWriter::FileWriter()
{
}


FileWriter::~FileWriter()
{
}


void FileWriter::def_dims(const vector<Dimension*> &dims)
{
    vector<Dimension*>::const_iterator dim_it;
    for (dim_it=dims.begin(); dim_it!=dims.end(); ++dim_it) {
        def_dim(*dim_it);
    }
}


void FileWriter::def_vars(const vector<Variable*> &vars)
{
    vector<Variable*>::const_iterator var_it;
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        def_var(*var_it);
    }
}


void FileWriter::copy_atts(const vector<Attribute*> &atts, const string &name)
{
    vector<Attribute*>::const_iterator att_it;
    for (att_it=atts.begin(); att_it!=atts.end(); ++att_it) {
        copy_att(*att_it, name);
    }
}


ostream& operator << (ostream &os, const FileWriter *writer)
{
    return writer->print(os);
}
