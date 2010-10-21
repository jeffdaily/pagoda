#ifndef PGEACOMMANDS_H_
#define PGEACOMMANDS_H_

#include "GenericCommands.H"


/**
 * All command-line parsing for the subsetter.
 */
class PgeaCommands : public GenericCommands
{
    public:
        PgeaCommands();
        PgeaCommands(int argc, char **argv);
        ~PgeaCommands();

        void parse(int argc, char **argv);

        vector<Dataset*> get_datasets();
        vector<vector<Variable*> > get_variables(const vector<Dataset*> &datasets);
        vector<vector<Dimension*> > get_dimensions(const vector<Dataset*> &datasets);

        FileWriter* get_output() const;

        string get_operator() const;

    protected:
        void init();

        Dataset* get_dataset(); // disable and override this method

        static vector<string> VALID;

        string op_type;
};

#endif // PGEACOMMANDS_H_
