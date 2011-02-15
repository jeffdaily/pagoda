#ifndef PGBOCOMMANDS_H_
#define PGBOCOMMANDS_H_

#include "GenericCommands.H"


/**
 * All command-line parsing for the subsetter.
 */
class PgboCommands : public GenericCommands
{
    public:
        PgboCommands();
        PgboCommands(int argc, char **argv);
        virtual ~PgboCommands();

        virtual void parse(int argc, char **argv);

        void get_inputs_and_outputs(Dataset* &dataset,
                                    Dataset* &operand,
                                    vector<Variable*> &vars,
                                    FileWriter* &writer);

        Dataset* get_operand() const;
        string get_operator() const;

    protected:
        void init();

        static vector<string> ADD;
        static vector<string> SUB;
        static vector<string> MUL;
        static vector<string> DIV;

        string op_type;
        string operand_filename;
};

#endif // PGBOCOMMANDS_H_
