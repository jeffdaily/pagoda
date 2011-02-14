#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <sstream>

#include "Attribute.H"
#include "DataType.H"
#include "Util.H"
#include "Values.H"
#include "TypedValues.H"

using std::ostringstream;


Attribute::Attribute()
{
}


Attribute::~Attribute()
{
}


size_t Attribute::get_count() const
{
    return get_values()->size();
}


string Attribute::get_string() const
{
    string ret;


    get_values()->as(ret);

    return ret;
}


int64_t Attribute::get_bytes() const
{
    return get_count() * get_type().get_bytes();
}


ostream& Attribute::print(ostream &os) const
{
    const string name = get_name();
    const DataType type = get_type();
    const string str = get_string();


    return os << name << "(" << type << ") = " << str;
}


ostream& operator << (ostream &os, const Attribute *other)
{
    return other->print(os);
}
