#ifndef FILEWRITER_H_
#define FILEWRITER_H_

#include <iostream>
#include <string>
#include <vector>

class Attribute;
class Dimension;
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
 * time) in order to write large, contiguous chunks of data.
 */
class FileWriter
{
    public:
        static FileWriter* create(const string &filename);

        FileWriter();
        virtual ~FileWriter();

        virtual void def_dim(Dimension *dim) = 0;
        virtual void def_dims(const vector<Dimension*> &dims);

        virtual void def_var(Variable *var) = 0;
        virtual void def_vars(const vector<Variable*> &vars);

        virtual void copy_att(Attribute *att,
                const string &name=string("")) = 0;
        virtual void copy_atts(const vector<Attribute*> &atts,
                const string &name=string(""));

        virtual void write(int handle, const string &name, int record=-1) = 0;

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const FileWriter *writer);
};

#endif // FILEWRITER_H_
