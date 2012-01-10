#ifndef COMMANDLINEPARSER_H_
#define COMMANDLINEPARSER_H_

#include <map>
#include <string>
#include <vector>

using std::map;
using std::string;
using std::vector;

class CommandLineOption;
typedef vector<CommandLineOption*> optvec_t;
typedef map<string,CommandLineOption*> optmap_t;


/**
 * Common command-line parsing capabilities are found here.
 */
class CommandLineParser
{
    public:
        CommandLineParser();
        ~CommandLineParser();

        void push_back(CommandLineOption &option);
        void erase(CommandLineOption &option);

        void parse(int argc, char **argv);

        int count(const string &name) const;
        int count(const CommandLineOption &option) const;

        string get_argument(const string &name) const;
        string get_argument(const CommandLineOption &option) const;

        vector<string> get_arguments(const string &name) const;
        vector<string> get_arguments(const CommandLineOption &option) const;

        vector<string> get_positional_arguments() const;

        string get_usage() const;

    protected:
        optvec_t options;
        optmap_t options_map;
        vector<string> positional_arguments;
};

#endif // COMMANDLINEPARSER_H_
