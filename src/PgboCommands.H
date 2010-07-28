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
        ~PgboCommands();

        void parse(int argc, char **argv);

        Dataset* get_dataset() const;
        Dataset* get_operand() const;
        string get_operator() const;

    protected:
        void init();

        static vector<string> ADD;
        static vector<string> SUB;
        static vector<string> MUL;
        static vector<string> DIV;

        string op_type;
};

#endif // PGBOCOMMANDS_H_
