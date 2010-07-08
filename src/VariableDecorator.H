#ifndef VARIABLEDECORATOR_H_
#define VARIABLEDECORATOR_H_

#include <iostream>
#include <string>
#include <vector>

#include "DataType.H"
#include "Variable.H"

using std::ostream;
using std::string;
using std::vector;

class Attribute;
class Dimension;
class Request;


class VariableDecorator : public Variable
{
    public:
        VariableDecorator(Variable *variable);
        virtual ~VariableDecorator();

        virtual string get_name() const;

        virtual vector<Dimension*> get_dims() const;
        virtual bool has_record() const;
        virtual int64_t get_ndim() const;
        virtual vector<int64_t> get_shape() const;

        virtual vector<Attribute*> get_atts() const;
        virtual int64_t num_atts() const;
        virtual string get_standard_name() const;
        virtual string get_long_name() const;
        virtual Attribute* get_att(const string &name,
                bool ignore_case=false) const;
        virtual Attribute* get_att(const vector<string> &names,
                bool ignore_case=false) const;

        virtual Dataset* get_dataset() const;

        virtual DataType get_type() const;

        virtual Array* read() const;
        virtual Array* read(Array *dst) const;
        virtual Array* read(int64_t record) const;
        virtual Array* read(int64_t record, Array *dst) const;

        virtual Array* iread();
        virtual Array* iread(Array *dst);
        virtual Array* iread(int64_t record);
        virtual Array* iread(int64_t record, Array *dst);

        virtual ostream& print(ostream &os) const;

        Variable* get_var() const { return var; }
    protected:
        Variable *var;
};

#endif // VARIABLEDECORATOR_H_
