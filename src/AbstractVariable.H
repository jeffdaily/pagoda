#ifndef ABSTRACTVARIABLE_H_
#define ABSTRACTVARIABLE_H_

#include <iostream>
#include <string>
#include <vector>

#include "DataType.H"
#include "Variable.H"

using std::ostream;
using std::string;
using std::vector;

class Array;
class Attribute;


/**
 * Variable operations which can be implemented using Variable's own methods
 * are done here.
 */
class AbstractVariable : public Variable
{
    public:
        virtual ~AbstractVariable();

        virtual string get_name() const = 0;

        virtual vector<Dimension*> get_dims() const = 0;
        virtual bool has_record() const;
        virtual int64_t num_dims() const;
        virtual vector<int64_t> get_shape() const;

        virtual vector<Attribute*> get_atts() const = 0;
        virtual int64_t num_atts() const;
        virtual string get_long_name() const;
        virtual Attribute* get_att(const string &name,
                bool ignore_case=false) const;
        virtual Attribute* get_att(const vector<string> &names,
                bool ignore_case=false) const;

        virtual DataType get_type() const = 0;

        virtual Array* read();
        virtual Array* read(Array *dst) = 0;
        virtual Array* read(int64_t record);
        virtual Array* read(int64_t record, Array *dst) = 0;

        virtual Array* iread();
        virtual Array* iread(Array *dst) = 0;
        virtual Array* iread(int64_t record);
        virtual Array* iread(int64_t record, Array *dst) = 0;

        virtual ostream& print(ostream &os) const;

    protected:
        AbstractVariable();
};

#endif // ABSTRACTVARIABLE_H_
