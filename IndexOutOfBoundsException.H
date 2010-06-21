#ifndef INDEXOUTOFBOUNDSEXCEPTION_H_
#define INDEXOUTOFBOUNDSEXCEPTION_H_

#include <stdexcept>

using std::runtime_error;


/**
 * Indicates an invalid index.
 */
class IndexOutOfBoundsException : public runtime_error
{
  public:
    IndexOutOfBoundsException(const string& s)
      : runtime_error(s) {}
};

#endif // INDEXOUTOFBOUNDSEXCEPTION_H_
