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

class Attribute;


/**
 * Variable operations which can be implemented using Variable's own methods
 * are done here.
 */
class AbstractVariable : public Variable
{
    public:
        virtual ~AbstractVariable();

        virtual bool has_record() const;
        virtual int64_t num_dims() const;
        virtual vector<int64_t> get_shape() const;

        virtual int64_t num_atts() const;
        virtual string get_long_name() const;
        virtual Attribute* find_att(const string &name,
                bool ignore_case=false) const;
        virtual Attribute* find_att(const vector<string> &names,
                bool ignore_case=false) const;

        virtual ostream& print(ostream &os) const;

    protected:
        AbstractVariable();
};

#endif // ABSTRACTVARIABLE_H_
