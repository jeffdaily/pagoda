#ifndef SUBSETTERCOMMANDS_H_
#define SUBSETTERCOMMANDS_H_

#include <string>
#include <vector>

#include "GenericCommands.H"

using std::string;
using std::vector;

class Dataset;
class Dimension;
class Variable;


/**
 * All command-line parsing for the subsetter.
 */
class SubsetterCommands : public GenericCommands
{
    public:
        SubsetterCommands();
        SubsetterCommands(int argc, char **argv);
        virtual ~SubsetterCommands();

        virtual void parse(int argc, char **argv);

        virtual Dataset* get_dataset();
        virtual vector<Variable*> get_variables(Dataset *dataset);
        virtual vector<Dimension*> get_dimensions(Dataset *dataset);

        virtual string get_join_name() const;
        virtual bool is_alphabetizing() const;

    protected:
        void init();

        string join_name;
        bool alphabetize;
};

#endif // SUBSETTERCOMMANDS_H_
