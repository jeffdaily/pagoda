#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "GenericCommands.H"
#include "SubsetterCommands.H"
#include "Timing.H"


SubsetterCommands::SubsetterCommands()
    :   GenericCommands()
{
}


SubsetterCommands::SubsetterCommands(int argc, char **argv)
    :   GenericCommands()
{
    parse(argc, argv);
}


SubsetterCommands::~SubsetterCommands()
{
}
