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


ostream& operator << (ostream &os, const Attribute *other)
{
    return other->print(os);
}

