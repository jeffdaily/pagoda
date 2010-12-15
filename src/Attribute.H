#ifndef ATTRIBUTE_H_
#define ATTRIBUTE_H_

#include <iostream>
#include <string>

#include "DataType.H"

using std::ostream;
using std::string;

class Attribute;
class Dataset;
class Values;
class Variable;


/**
 * Attributes may be associated with Variables or Datasets.
 *
 * An Attribute is a typed local array of Values.
 *
 * An attribute is meant to describe Variables or Datasets with additional
 * properties such as units, special values, max and min valid ranges, scaling
 * factors, offsets, etc.
 *
 */
class Attribute
{
    public:
        virtual ~Attribute();

        virtual string get_name() const = 0;
        virtual DataType get_type() const = 0;
        virtual Values* get_values() const = 0;

        virtual Dataset* get_dataset() const = 0;
        virtual Variable* get_var() const = 0;

        virtual size_t get_count() const;
        virtual string get_string() const;
        virtual int64_t get_bytes() const;

        virtual ostream& print(ostream &os) const;
        friend ostream& operator << (ostream &os, const Attribute *other);

    protected:
        Attribute();
};

#endif // ATTRIBUTE_H_
