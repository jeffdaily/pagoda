#ifndef COMMANDLINEPARSER_H_
#define COMMANDLINEPARSER_H_

#include <map>
#include <sstream>
#include <string>
#include <vector>

using std::istringstream;
using std::map;
using std::string;
using std::vector;

class CommandLineOption;
class CommandLineParserValue;
typedef vector<CommandLineOption*> optvec_t;
typedef map<string,CommandLineOption*> optmap_t;


/**
 * Common command-line parsing capabilities are found here.
 */
class CommandLineParser
{
    public:
        CommandLineParser();
        virtual ~CommandLineParser();

        virtual void push_back(CommandLineOption *option);
        virtual void parse(int argc, char **argv);
        virtual int count(const string &name) const;
        virtual string get_argument(const string &name) const;
        virtual vector<string> get_arguments(const string &name) const;

        virtual string get_usage() const;

    protected:
        optvec_t options;
        optmap_t options_map;
        vector<string> positional_arguments;
};


/**
 * Holds pointer to value on command line.
 */
class CommandLineParserValue
{
    public:
        virtual ~CommandLineParserValue();

        virtual void parse(const vector<string> &vec);

    protected:
        CommandLineParserValue();
};


/**
 * Generic implementation of CommandLineParserValue.
 */
template <class T>
class CommandLineParserValueGeneric : public CommandLineParserValue
{
    public:
        CommandLineParserValueGeneric(T *value)
            :   CommandLineParserValue()
            ,   value(value)
            ,   values(NULL)
        {
        }

        CommandLineParserValueGeneric(vector<T> *value)
            :   CommandLineParserValue()
            ,   value(NULL)
            ,   values(value)
        {
        }

        virtual ~CommandLineParserValueGeneric()
        {
            value = NULL;
            values = NULL;
        }

        virtual void parse(const vector<string> &vec)
        {
            if (vec.empty()) {
                if (value) {
                    *value = 1; // true
                } else if (values) {
                }
            } else {
                if (value) {
                    istringstream iss(vec.front());
                    iss >> *value;
                } else if (values) {
                    for (vector<string>::const_iterator it=vec.begin();
                            it!=vec.end(); ++it) {
                        T current;
                        istringstream iss(*it);
                        iss >> current;
                        values->push_back(current);
                    }
                }
            }
        }

    protected:
        T *value;
        vector<T> *values;
};

#endif // COMMANDLINEPARSER_H_
