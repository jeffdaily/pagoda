#ifndef PGRCATCOMMANDS_H_
#define PGRCATCOMMANDS_H_

#include "GenericCommands.H"


/**
 * All command-line parsing for the pgrcat record concatenator.
 */
class PgrcatCommands : public GenericCommands
{
    public:
        PgrcatCommands();
        PgrcatCommands(int argc, char **argv);
        virtual ~PgrcatCommands();

        virtual Dataset* get_dataset();

    protected:
        void init();
};

#endif // PGRCATCOMMANDS_H_
