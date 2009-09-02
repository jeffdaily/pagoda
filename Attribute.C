#include <sstream>

#include "Attribute.H"
#include "Common.H"
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
    ostringstream os;
    os << get_values();
    return os.str();
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

