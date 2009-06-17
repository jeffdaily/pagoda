#ifndef UTIL_H_
#define UTIL_H_

#include <iostream>
    using std::endl;
#include <sstream>
    using std::ostringstream;
#include <string>
    using std::string;

#include "SubsetterException.H"

#define ME GA_Nodeid()
#define MAX_NAME 80
#define MAX_DIM 8

#define ERR(e) { \
ostringstream __os; \
__os << "Error: " << e << endl; \
throw SubsetterException(__os.str()); }

#define ERRNO(n) { \
ostringstream __os; \
__os << "Error: " << ncmpi_strerror(n) << endl; \
throw SubsetterException(__os.str()); }

#define ERRNO_CHECK(n) \
  if (n != NC_NOERR) { \
    ERRNO(n); \
  }

//#define DEBUG
#ifdef DEBUG
#include <assert.h>
//#define DEBUG_MASKS
#define DEBUG_PRINT fprintf
#define DEBUG_PRINT_ME if (ME == 0) fprintf
#else
#define DEBUG_PRINT
#define DEBUG_PRINT_ME
#endif



class StringUtil
{
    public:
        static bool ends_with(const string &str, const string &end);
};

#endif // UTIL_H_
