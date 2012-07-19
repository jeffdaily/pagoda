#ifndef PGWACOMMANDS_H_
#define PGWACOMMANDS_H_

#include <set>

#include "GenericCommands.H"

using std::set;


/**
 * All command-line parsing for the subsetter.
 */
class PgwaCommands : public GenericCommands
{
    public:
        PgwaCommands();
        PgwaCommands(int argc, char **argv);
        virtual ~PgwaCommands();

        virtual void parse(int argc, char **argv);

        virtual void get_inputs_and_outputs(Dataset* &dataset,
                                            vector<Variable*> &vars,
                                            FileWriter* &writer);

        string get_operator() const;
        Variable* get_mask_variable();
        Variable* get_weight_variable();
        set<string> get_reduced_dimensions() const;

    protected:
        void init();

        static vector<string> VALID;

        string op_type;
        string mask_value;
        string mask_variable;
        string mask_comparator;
        string weight_variable;
        set<string> dimensions;
        bool retain_degenerate_dimensions;
};

#endif // PGWACOMMANDS_H_
