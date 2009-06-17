#ifndef ATTRIBUTE_H_
#define ATTRIBUTE_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;

#include <pnetcdf.h>

#include "DataType.H"


class Attribute
{
    public:
        Attribute();
        Attribute(int ncid, int attid, int varid=NC_GLOBAL, string file="");
        Attribute(const Attribute &copy);
        virtual ~Attribute();

        string get_name() const;
        DataType get_type() const;
        long get_count() const;
        bool is_global() const;

        bool operator == (const Attribute &other) const;
        friend ostream& operator << (ostream &os, const Attribute &other);

    private:
        string name;
        DataType type;
        long count;
        bool global;
        string file;
        string variable;
};

#endif // ATTRIBUTE_H_
