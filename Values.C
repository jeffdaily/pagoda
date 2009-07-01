#include "Values.H"

Values::Values()
{
}


Values::~Values()
{
}


ostream& operator << (ostream &os, const Values *values)
{
    return values->print(os);
}

