#ifndef COMMANDEXCEPTION_H_
#define COMMANDEXCEPTION_H_

#include <string>

#include "PagodaException.H"

using std::string;


/**
 * A pagoda error as a result of bad command-line arguments or parsing.
 *
 * Fatal, but such that pagoda::abort() is not necessary.
 */
class CommandException : public PagodaException
{
  public:
      explicit CommandException(const string &what)
          : PagodaException(what) {}

      explicit CommandException(const string &where, const string &what)
          : PagodaException(where, what) {}

      explicit CommandException(const string &where, const string &what,
              int value)
          : PagodaException(where, what, value) {}
};

#endif // COMMANDEXCEPTION_H_
