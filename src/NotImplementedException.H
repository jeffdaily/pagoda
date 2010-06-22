#ifndef NOTIMPLEMENTEDEXCEPTION_H_
#define NOTIMPLEMENTEDEXCEPTION_H_

#include <stdexcept>

using std::runtime_error;


/**
 * A run-time placeholder for unimplemented functionality.
 */
class NotImplementedException : public runtime_error
{
  public:
    NotImplementedException(const string& s)
      : runtime_error(s) {}
};

#endif // NOTIMPLEMENTEDEXCEPTION_H_
