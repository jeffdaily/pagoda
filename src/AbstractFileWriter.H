#ifndef ABSTRACTFILEWRITER_H_
#define ABSTRACTFILEWRITER_H_

#include <stdint.h>

#include <iostream>
#include <string>
#include <vector>

#include "FileFormat.H"
#include "FileWriter.H"

class Array;
class Attribute;
class DataType;
class Dataset;
class Dimension;
class Values;
class Variable;

using std::ostream;
using std::string;
using std::vector;


class AbstractFileWriter : public FileWriter
{
    public:
        AbstractFileWriter();
        virtual ~AbstractFileWriter();
        virtual void close() = 0;

        // named parameter idiom, but using virtuals
        virtual FileWriter* create() = 0; // finalizes parameters
        virtual FileWriter* fixed_record_dimension(int value) = 0;
        virtual FileWriter* header_pad(int value) = 0;
        virtual FileWriter* file_format(FileFormat value) = 0;
        virtual FileWriter* append(bool value) = 0;
        virtual FileWriter* overwrite(bool value) = 0;

        virtual void def_dataset(Dataset *dataset);

        virtual void def_dim(const string &name, int64_t size) = 0;
        virtual void def_dim(Array *mask);
        virtual void def_dim(Dimension *dim);
        virtual void def_dims(const vector<Array*> &masks);
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
                             const vector<Array*> &dims,
                             const DataType &type,
                             const vector<Attribute*> &atts=vector<Attribute*>());
        virtual void def_var(Variable *var);
        virtual void def_vars(const vector<Variable*> &vars);

        virtual void write_att(const string &name, Values *values,
                               DataType type,
                               const string &var_name=string(""));
        virtual void write_att(Attribute *att,
                               const string &name=string("")) = 0;
        virtual void write_atts(const vector<Attribute*> &atts,
                                const string &name=string(""));

        virtual void write(Array *array, const string &name) = 0;
        virtual void write(Array *array, const string &name,
                           int64_t record) = 0;
        virtual void write(Array *array, const string &name,
                           const vector<int64_t> &start) = 0;

        virtual void iwrite(Array *array, const string &name) = 0;
        virtual void iwrite(Array *array, const string &name,
                            int64_t record) = 0;
        virtual void iwrite(Array *array, const string &name,
                            const vector<int64_t> &start) = 0;

        virtual void copy_vars(const vector<Variable*> &vars);
        virtual void icopy_vars(const vector<Variable*> &vars);

        virtual void wait() = 0;

        virtual ostream& print(ostream &os) const = 0;
};

#endif // ABSTRACTFILEWRITER_H_
