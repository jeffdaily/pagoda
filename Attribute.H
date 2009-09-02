#ifndef ATTRIBUTE_H_
#define ATTRIBUTE_H_

#include <iostream>
#include <string>

#include "DataType.H"

using std::ostream;
using std::string;

class Attribute;
class Values;


class Attribute
{
    public:
        virtual ~Attribute();

        virtual string get_name() const = 0;
        virtual DataType get_type() const = 0;
        virtual bool is_global() const = 0;
        virtual Values* get_values() const = 0;
        virtual size_t get_count() const;
        virtual string get_string() const;

        virtual ostream& print(ostream &os) const;
        friend ostream& operator << (ostream &os, const Attribute *other);

    protected:
        Attribute();
};

#endif // ATTRIBUTE_H_
