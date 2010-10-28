#ifndef RANGEEXCEPTION_H_
#define RANGEEXCEPTION_H_

#include <stdexcept>

using std::runtime_error;


/**
 * Indicate invalid range.
 */
class RangeException : public runtime_error
{
    public:
        RangeException(const string& s)
            : runtime_error(s) {}
};

#endif // RANGEEXCEPTION_H_
