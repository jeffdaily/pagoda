#ifndef SUBSETTEREXCEPTION_H_
#define SUBSETTEREXCEPTION_H_

#include <stdexcept>
#include <string>

using std::runtime_error;
using std::string;


/**
 * A subsetter-specific error.
 */
class SubsetterException : public runtime_error
{
  public:
    SubsetterException(const string &where, const string& s)
      : runtime_error(where + ": " + s) {}
};

#endif // SUBSETTEREXCEPTION_H_
