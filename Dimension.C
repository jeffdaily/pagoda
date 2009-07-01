#include "Dimension.H"

Dimension::Dimension()
{
}


Dimension::~Dimension()
{
}


ostream& operator << (ostream &os, const Dimension *other)
{
    return other->print(os);
}

