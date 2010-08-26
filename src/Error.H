#ifndef ERROR_H_
#define ERROR_H_

#include <iostream>
#include <sstream>

using std::cerr;
using std::endl;
using std::ostringstream;

#include "PagodaException.H"


#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define AT string(__FILE__ ":" TOSTRING(__LINE__))

#define ERR(e) \
do { \
    ostringstream __os; \
    const string __prefix("Error: "); \
    __os << __prefix << e << endl; \
    const string __msg(__os.str()); \
    const string __where(AT); \
    cerr << __where << ": " << __msg << endl; \
    throw PagodaException(__where, __msg); \
} while (0)

#define ERRCODE(e,code) \
do { \
    ostringstream __os; \
    const string __prefix("Error: "); \
    __os << __prefix << e << endl; \
    const string __msg(__os.str()); \
    const string __where(AT); \
    cerr << __where << ": " << __msg << endl; \
    throw PagodaException(__where, __msg, code); \
} while (0)

#define ASSERT(e) \
    if (e) { \
        ERR("assertion failed"); \
    } \

#endif // ERROR_H_
