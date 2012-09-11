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

        string get_unlimited_name() const;

    protected:
        void init();

        string unlimited_name;
};

#endif // PGECATCOMMANDS_H_
