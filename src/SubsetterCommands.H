#ifndef SUBSETTERCOMMANDS_H_
#define SUBSETTERCOMMANDS_H_

#include <string>

#include "GenericCommands.H"

using std::string;


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

        virtual string get_join_name() const;

    protected:
        void init();

        virtual Dataset* get_dataset();

        string join_name;
};

#endif // SUBSETTERCOMMANDS_H_
