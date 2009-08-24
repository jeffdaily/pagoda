#include "TypedValues.H"

ostream& __print(ostream &os, const TypedValues<char> &obj)
{
    //os << '"';
    size_t i,limit;
    for (i=0,limit=obj.size(); i<limit; ++i) {
        char val = obj.at(i);
        if (val == '\0') break;
        os << val;
    }
    //os << '"';
    return os;
}
