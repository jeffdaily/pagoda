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

        virtual FileWriter* get_output() const;

        vector<Dataset*> get_datasets();
        vector<vector<Variable*> > get_variables(const vector<Dataset*> &datasets);
        vector<vector<Dimension*> > get_dimensions(const vector<Dataset*> &datasets);

        string get_unlimited_name() const;

    protected:
        void init();

        virtual Dataset* get_dataset(); // disable and override this method

        string unlimited_name;
};

#endif // PGECATCOMMANDS_H_
