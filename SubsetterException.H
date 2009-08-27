#ifndef SUBSETTEREXCEPTION_H_
#define SUBSETTEREXCEPTION_H_

#include <stdexcept>
  using std::runtime_error;

class SubsetterException : public runtime_error
{
  public:
    SubsetterException(const string &where, const string& s)
      : runtime_error(where + ": " + s) {}
};

#endif // SUBSETTEREXCEPTION_H_
