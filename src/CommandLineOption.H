#ifndef COMMANDLINEOPTION_H_
#define COMMANDLINEOPTION_H_

#include <string>
#include <vector>

using std::string;
using std::vector;


static string OP_AVG("avg");
static string OP_SQRAVG("sqravg");
static string OP_AVGSQR("avgsqr");
static string OP_MAX("max");
static string OP_MIN("min");
static string OP_RMS("rms");
static string OP_RMSSDN("rmssdn");
static string OP_SQRT("sqrt");
static string OP_TTL("ttl");
static string OP_ADD("+");
static string OP_DIV("/");
static string OP_MUL("*");
static string OP_SUB("-");

class CommandLineOption
{
    public:
        static int WIDTH;

        CommandLineOption(const int &value, const string &name,
                const bool &argument, const string &description);
        virtual ~CommandLineOption();

        virtual void set_value(const int &value);
        virtual void set_name(const string &name);
        virtual void has_argument(const bool &argument);
        virtual void set_description(const string &description);

        virtual int get_value() const;
        virtual string get_name() const;
        virtual bool has_argument() const;
        virtual string get_description() const;

        virtual bool handle(int value, const string &arg,
                const string &name="");
        virtual string get_usage() const;
        virtual string get_option() const;
        virtual struct option get_option_long() const;
        virtual int get_count() const;
        virtual string get_argument() const;
        virtual vector<string> get_arguments() const;

        static struct option get_long_opt_null();

        static CommandLineOption HELP;
        static CommandLineOption INPUT_PATH;
        static CommandLineOption OUTPUT;
        static CommandLineOption VARIABLE;
        static CommandLineOption EXCLUDE;
        static CommandLineOption NO_COORDS;
        static CommandLineOption COORDS;
        static CommandLineOption TOPOLOGY;
        static CommandLineOption DIMENSION;
        static CommandLineOption AUXILIARY;
        static CommandLineOption LATLONBOX;
        static CommandLineOption OPERATION;
        static CommandLineOption OVERWRITE;
        static CommandLineOption APPEND;
        static CommandLineOption HISTORY;
        static CommandLineOption ALPHABETIZE;
        static CommandLineOption JOIN;
        static CommandLineOption UNION;
        static CommandLineOption OP_TYPE;
        static CommandLineOption AVG_TYPE;

    protected:
        int _value;
        string _name;
        bool _has_argument;
        string _description;

        int _count;
        vector<string> _arguments;
};

#endif // COMMANDLINEOPTION_H_
