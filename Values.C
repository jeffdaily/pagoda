#include <sstream>
    using std::ostringstream;

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


#define implement_as(TYPE) \
void Values::as(TYPE* &values) const \
{ \
    size_t limit = size(); \
    values = new TYPE[limit]; \
    for (size_t i=0; i<limit; ++i) { \
        as(i, values[i]); \
    } \
}
implement_as(char)
implement_as(signed char)
implement_as(unsigned char)
implement_as(short)
implement_as(int)
implement_as(long)
implement_as(float)
implement_as(double)
#undef implement_as


void Values::as(string &values) const
{
    ostringstream os;
    size_t i,limit;
    for (i=0,limit=size(); i<limit; ++i) {
        string val;
        as(i, val);
        if (val == "\0") break;
        os << val;
    }
    values = os.str();
}

