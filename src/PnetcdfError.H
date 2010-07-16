#ifndef PNETCDFERROR_H_
#define PNETCDFERROR_H_

#include <sstream>

#include "Error.H"
#include "PagodaException.H"

using std::endl;
using std::ostringstream;

#define ERRNO(n) \
do { \
    ostringstream __os; \
    const string __prefix("Error: "); \
    const string __errstr(ncmpi_strerror(n)); \
    __os << __prefix << __errstr << endl; \
    const string __msg(__os.str()); \
    const string __where(AT); \
    throw PagodaException(__where, __msg); \
} while (0)

#define ERRNO_CHECK(n) \
do { \
    if (NC_NOERR != n) { \
        ERRNO(n); \
    } \
} while (0)

#endif // PNETCDFERROR_H_
