#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <iostream>
#include <string>
#include <vector>

#include "FileWriter.H"
#include "NetcdfFileWriter.H"
#include "Util.H"
#include "Timing.H"

using std::ostream;
using std::string;
using std::vector;


FileWriter* FileWriter::create(const string &filename)
{
    //TIMING("FileWriter::create(string)");
    FileWriter *writer = NULL;
    string EXT_NC(".nc");
    if (pagoda::ends_with(filename, EXT_NC)) {
        writer = new NetcdfFileWriter(filename);
    }
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


void FileWriter::def_dims(const vector<Dimension*> &dims)
{
    TIMING("FileWriter::def_dims(vector<Dimension*>)");
    vector<Dimension*>::const_iterator dim_it;
    for (dim_it=dims.begin(); dim_it!=dims.end(); ++dim_it) {
        def_dim(*dim_it);
    }
}


void FileWriter::def_vars(const vector<Variable*> &vars)
{
    TIMING("FileWriter::def_vars(vector<Variable*>)");
    vector<Variable*>::const_iterator var_it;
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        def_var(*var_it);
    }
}


void FileWriter::copy_atts(const vector<Attribute*> &atts, const string &name)
{
    TIMING("FileWriter::copy_atts(vector<Attribute*>,string)");
    vector<Attribute*>::const_iterator att_it;
    for (att_it=atts.begin(); att_it!=atts.end(); ++att_it) {
        copy_att(*att_it, name);
    }
}


ostream& operator << (ostream &os, const FileWriter *writer)
{
    TIMING("operator<<(ostream,FileWriter*)");
    return writer->print(os);
}
