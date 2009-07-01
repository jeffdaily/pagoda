#ifndef VARIABLE_H_
#define VARIABLE_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;
#include <vector>
    using std::vector;

#include "DataType.H"

class Attribute;
class Dimension;


class Variable
{
    public:
        Variable();
        virtual ~Variable();

        virtual string get_name() const = 0;

        virtual bool has_record() const = 0;
        virtual size_t num_dims() const = 0;
        virtual vector<Dimension*> get_dims() const = 0;

        virtual size_t num_atts() const = 0;
        virtual vector<Attribute*> get_atts() const = 0;

        virtual DataType get_type() const = 0;

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Variable *other);
};

#endif // VARIABLE_H_
