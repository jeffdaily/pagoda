#ifndef PAGODAEXCEPTION_H_
#define PAGODAEXCEPTION_H_

#include <stdexcept>
#include <sstream>
#include <string>

using std::ostringstream;
using std::runtime_error;
using std::string;


/**
 * A generic pagoda error.
 */
class PagodaException : public runtime_error
{
  public:
      static string to_string(int value) {
          ostringstream os;
          os << value;
          return os.str();
      }

      explicit PagodaException(const string &what)
          : runtime_error(what) {}

      explicit PagodaException(const string &where, const string &what)
          : runtime_error(where + ": " + what) {}

      explicit PagodaException(const string &where, const string &what,
              int value)
          : runtime_error(where + ": " + what + " :: " + to_string(value)) {}
};

#endif // PAGODAEXCEPTION_H_
