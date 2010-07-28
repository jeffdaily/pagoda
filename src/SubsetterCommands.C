#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "GenericCommands.H"
#include "SubsetterCommands.H"
#include "Timing.H"


SubsetterCommands::SubsetterCommands()
    :   GenericCommands()
{
    TIMING("SubsetterCommands::SubsetterCommands()");
}


SubsetterCommands::SubsetterCommands(int argc, char **argv)
    :   GenericCommands()
{
    TIMING("SubsetterCommands::SubsetterCommands(int,char**)");
    parse(argc, argv);
}


SubsetterCommands::~SubsetterCommands()
{
    TIMING("SubsetterCommands::~SubsetterCommands()");
}
