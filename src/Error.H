#ifndef ERROR_H_
#define ERROR_H_

#include <iostream>
#include <sstream>

using std::endl;
using std::ostringstream;

#include "PagodaException.H"
#include "Util.H"


#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define AT string(__FILE__ ":" TOSTRING(__LINE__))

#define EXCEPT(__exception, __message, __code) \
do { \
    ostringstream __os; \
    const string __prefix("Error: "); \
    __os << __prefix << (__message) << endl; \
    const string __msg(__os.str()); \
    const string __where(AT); \
    pagoda::print_backtrace(); \
    throw __exception(__where, __msg, (__code)); \
} while (0)

#define ERR(e) EXCEPT(PagodaException,e,0)

#define ERRCODE(e,code) EXCEPT(PagodaException,e,code)

#define ASSERT(e) \
if (e) { \
    ERR("assertion failed"); \
} \

#endif // ERROR_H_
