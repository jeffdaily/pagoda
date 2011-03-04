#ifndef PGFLINTCOMMANDS_H_
#define PGFLINTCOMMANDS_H_

#include "GenericCommands.H"

#define ENABLE_INTERPOLANT 0


/**
 * All command-line parsing for the subsetter.
 */
class PgflintCommands : public GenericCommands
{
    public:
        PgflintCommands();
        PgflintCommands(int argc, char **argv);
        virtual ~PgflintCommands();

        virtual void parse(int argc, char **argv);

        void get_inputs_and_outputs(Dataset* &dataset,
                                    Dataset* &operand,
                                    vector<Variable*> &vars,
                                    FileWriter* &writer);

        Dataset* get_operand() const;

#if ENABLE_INTERPOLANT
        bool has_interpolant() const;
        string get_interpolant() const;
        double get_value() const;
#endif
        double get_weight1() const;
        double get_weight2() const;

    protected:
        void init();

        string operand_filename;
#if ENABLE_INTERPOLANT
        string interpolant;
        double value;
#endif
        double weight1;
        double weight2;
};

#endif // PGFLINTCOMMANDS_H_
