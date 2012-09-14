#ifndef PGPDQCOMMANDS_H_
#define PGPDQCOMMANDS_H_

#include <string>
#include <vector>

#include "GenericCommands.H"

using std::string;
using std::vector;


/**
 * All command-line parsing for the pdq tool.
 */
class PgpdqCommands : public GenericCommands
{
    public:
        PgpdqCommands();
        PgpdqCommands(int argc, char **argv);
        virtual ~PgpdqCommands();

        virtual void parse(int argc, char **argv);

        virtual string get_join_name() const;
        virtual vector<string> get_permuted_names() const;

    protected:
        void init();

        virtual Dataset* get_dataset();

        string join_name;
        vector<string> permuted_names;
};

#endif // PGPDQCOMMANDS_H_
