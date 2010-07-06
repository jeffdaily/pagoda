#ifndef FILEWRITER_H_
#define FILEWRITER_H_

#include <iostream>
#include <string>
#include <vector>

class Array;
class Attribute;
class DataType;
class Dimension;
class Mask;
class Variable;

using std::ostream;
using std::string;
using std::vector;


/**
 * Interface for writing a Dataset to a file.
 *
 * Following the constraints of the netcdf format, the file must be completely
 * defined (all Dimensions, Variables, Variable Attributes, and global
 * Attributes) before writing of data can take place.
 *
 * As an optimization, it is recommended that for record Variables, the data
 * is written one complete record at a time (versus one entire Variable at a
 * time) in order to write large, contiguous chunks of data.  This can be
 * accomplished using the non-blocking interface.
 */
class FileWriter
{
    public:
        static FileWriter* create(const string &filename);

        FileWriter();
        virtual ~FileWriter();

        virtual void def_dim(const string &name, int64_t size) = 0;
        virtual void def_dim(Mask *mask);
        virtual void def_dim(Dimension *dim);
        virtual void def_dims(const vector<Mask*> &masks);
        virtual void def_dims(const vector<Dimension*> &dims);

        virtual void def_var(const string &name,
                const vector<string> &dims,
                const DataType &type,
                const vector<Attribute*> &atts=vector<Attribute*>()) = 0;
        virtual void def_var(const string &name,
                const vector<Dimension*> &dims,
                const DataType &type,
                const vector<Attribute*> &atts=vector<Attribute*>());
        virtual void def_var(const string &name,
                const vector<Mask*> &dims,
                const DataType &type,
                const vector<Attribute*> &atts=vector<Attribute*>());
        virtual void def_var(Variable *var);
        virtual void def_vars(const vector<Variable*> &vars);

        virtual void copy_att(Attribute *att,
                const string &name=string("")) = 0;
        virtual void copy_atts(const vector<Attribute*> &atts,
                const string &name=string(""));

        virtual void write(Array *array, const string &name) = 0;
        virtual void write(Array *array, const string &name,
                int64_t record) = 0;
        virtual void write(Array *array, const string &name,
                const vector<int64_t> &start) = 0;

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const FileWriter *writer);
};

#endif // FILEWRITER_H_
