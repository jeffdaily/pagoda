#ifndef GENERICATTRIBUTE
#define GENERICATTRIBUTE

#include <iostream>

#include "Attribute.H"
#include "DataType.H"

class Dataset;
class Values;
class Variable;

using std::ostream;


class GenericAttribute : public Attribute
{
    public:
        GenericAttribute(const string &name, Values *values, DataType type);
        virtual ~GenericAttribute();

        virtual string get_name() const;
        virtual DataType get_type() const;
        virtual Values* get_values() const;

        virtual Dataset* get_dataset() const;
        virtual Variable* get_var() const;

        virtual ostream& print(ostream &os) const;

    protected:
        string name;
        Values *values;
        DataType type;

};

#endif // GENERICATTRIBUTE
