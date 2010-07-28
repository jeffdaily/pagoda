#ifndef SUBSETTERCOMMANDS_H_
#define SUBSETTERCOMMANDS_H_

#include "GenericCommands.H"


/**
 * All command-line parsing for the subsetter.
 */
class SubsetterCommands : public GenericCommands
{
    public:
        SubsetterCommands();
        SubsetterCommands(int argc, char **argv);
        ~SubsetterCommands();
};

#endif // SUBSETTERCOMMANDS_H_
