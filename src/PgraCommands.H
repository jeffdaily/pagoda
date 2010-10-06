#ifndef PGRACOMMANDS_H_
#define PGRACOMMANDS_H_

#include "GenericCommands.H"


/**
 * All command-line parsing for the subsetter.
 */
class PgraCommands : public GenericCommands
{
    public:
        PgraCommands();
        PgraCommands(int argc, char **argv);
        ~PgraCommands();

        void parse(int argc, char **argv);

        FileWriter* get_output() const;

        string get_operator() const;

    protected:
        void init();

        static vector<string> VALID;

        string op_type;
};

#endif // PGRACOMMANDS_H_
