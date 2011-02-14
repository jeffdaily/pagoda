#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <iostream>
#include <string>
#include <vector>

#include "Timing.H"
#include "Values.H"

using std::ostream;
using std::string;
using std::vector;


Values::Values()
{
}


Values::~Values()
{
}


#define implement_as(TYPE) \
void Values::as(vector<TYPE> &values) const \
{ \
    size_t i = 0; \
    size_t limit = size(); \
    values.clear(); \
    values.reserve(limit); \
    for (i=0; i<limit; ++i) { \
        TYPE val; \
        as(i, val); \
        values.push_back(val); \
    } \
}
implement_as(char)
implement_as(signed char)
implement_as(unsigned char)
implement_as(short)
implement_as(unsigned short)
implement_as(int)
implement_as(unsigned int)
implement_as(long)
implement_as(unsigned long)
implement_as(long long)
implement_as(unsigned long long)
implement_as(float)
implement_as(double)
implement_as(long double)
#undef implement_as


void Values::as(string &values) const
{
    size_t i = 0;
    size_t limit = size();
    values.clear();
    values.reserve(limit);
    for (i=0; i<limit; ++i) {
        string val;
        as(i, val);
        values.append(val);
    }
}


ostream& operator << (ostream &os, const Values *values)
{
    return values->print(os);
}
