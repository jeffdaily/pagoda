#ifndef ATTRIBUTE_H_
#define ATTRIBUTE_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;

#include "DataType.H"

class Attribute;
class Values;

class Attribute
{
    public:
        Attribute();
        virtual ~Attribute();

        virtual string get_name() const = 0;
        virtual DataType get_type() const = 0;
        virtual size_t get_count() const = 0;
        virtual bool is_global() const = 0;
        virtual Values* get_values() const = 0;

        virtual ostream& print(ostream &os) const = 0;
        friend ostream& operator << (ostream &os, const Attribute *other);
};

#endif // ATTRIBUTE_H_
