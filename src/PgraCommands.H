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
        virtual ~PgraCommands();

        virtual void parse(int argc, char **argv);

        virtual Dataset* get_dataset();
        virtual FileWriter* get_output() const;

        string get_operator() const;

    protected:
        void init();

        static vector<string> VALID;

        string op_type;
};

#endif // PGRACOMMANDS_H_
