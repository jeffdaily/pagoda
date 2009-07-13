#ifndef VARIABLEDECORATOR_H_
#define VARIABLEDECORATOR_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "DataType.H"
#include "Variable.H"

class Attribute;
class Dimension;


class VariableDecorator : public Variable
{
    public:
        VariableDecorator(Variable *variable);
        virtual ~VariableDecorator();

        virtual string get_name() const;

        virtual bool has_record() const;
        virtual size_t num_dims() const;
        virtual vector<Dimension*> get_dims() const;
        virtual int64_t get_size() const;
        virtual int64_t* get_sizes() const;
        virtual size_t num_masks() const;

        virtual size_t num_atts() const;
        virtual vector<Attribute*> get_atts() const;
        virtual string get_long_name() const;
        virtual Attribute* find_att(const string &name,
                bool ignore_case=true) const;
        virtual Attribute* find_att(const vector<string> &names,
                bool ignore_case=true) const;

        virtual DataType get_type() const;

        virtual int get_handle();
        virtual void release_handle();
        virtual void read();
        virtual void set_record_index(size_t index);
        virtual size_t get_record_index() const;

        virtual ostream& print(ostream &os) const;

    protected:
        Variable *var;
};

#endif // VARIABLEDECORATOR_H_
