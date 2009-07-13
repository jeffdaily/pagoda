#include <sstream>
    using std::ostringstream;

#include "Attribute.H"
#include "Util.H"
#include "Values.H"
#include "TypedValues.H"


Attribute::Attribute()
{
}


Attribute::~Attribute()
{
}


string Attribute::get_string() const
{
    ostringstream os;
    os << get_values();
    return os.str();
}


ostream& operator << (ostream &os, const Attribute *other)
{
    return other->print(os);
}

