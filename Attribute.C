#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <sstream>

#include "Attribute.H"
#include "DataType.H"
#include "Util.H"
#include "Values.H"
#include "Timing.H"
#include "TypedValues.H"

using std::ostringstream;


Attribute::Attribute()
{
    TIMING("Attribute::Attribute()");
}


Attribute::~Attribute()
{
    TIMING("Attribute::~Attribute()");
}


size_t Attribute::get_count() const
{
    TIMING("Attribute::get_count()");
    return get_values()->size();
}


string Attribute::get_string() const
{
    TIMING("Attribute::get_string()");
    ostringstream os;
    os << get_values();
    return os.str();
}


ostream& Attribute::print(ostream &os) const
{
    TIMING("Attribute::print(ostream)");
    const string name = get_name();
    const DataType type = get_type();
    const string str = get_string();
    return os << name << "(" << type << ") = " << str;
}


ostream& operator << (ostream &os, const Attribute *other)
{
    TIMING("Attribute::operator<<(ostream,Attribute*)");
    return other->print(os);
}

