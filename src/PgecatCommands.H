#ifndef PGECATCOMMANDS_H_
#define PGECATCOMMANDS_H_

#include "GenericCommands.H"


/**
 * All command-line parsing for pgecat ensemble concatenator.
 */
class PgecatCommands : public GenericCommands
{
    public:
        PgecatCommands();
        PgecatCommands(int argc, char **argv);
        virtual ~PgecatCommands();

        virtual void parse(int argc, char **argv);

    protected:
        void init();

        string new_unlimited_name;
};

#endif // PGECATCOMMANDS_H_
