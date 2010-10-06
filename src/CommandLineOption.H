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

enum AverageOperator {
    AO_AVG,
    AO_SQRAVG,
    AO_AVGSQR,
    AO_MAX,
    AO_MIN,
    AO_RMS,
    AO_RMSSDN,
    AO_SQRT,
    AO_TTL,
};

enum BinaryOperator {
    BO_ADD,
    BO_DIV,
    BO_MUL,
    BO_SUB,
};

class CommandLineOption
{
    public:
        static int WIDTH;

        CommandLineOption(const int &value, const string &name,
                const bool &argument, const string &description,
                const string &extra_name1="", const string &extra_name2="");
        ~CommandLineOption();

        void set_value(const int &value);
        void set_name(const string &name);
        void set_names(const vector<string> &names);
        void has_argument(const bool &argument);
        void set_description(const string &description);

        int get_value() const;
        string get_name() const;
        vector<string> get_names() const;
        bool has_argument() const;
        string get_description() const;

        bool handle(int value, const string &arg, const string &name="");
        string get_usage() const;
        string get_option() const;
        struct option get_long_option() const;
        vector<struct option> get_long_options() const;
        int get_count() const;
        string get_argument() const;
        vector<string> get_arguments() const;

        static struct option get_long_opt_null();

        static CommandLineOption ALPHABETIZE;
        static CommandLineOption APPEND;
        static CommandLineOption AUXILIARY;
        static CommandLineOption AVG_TYPE;
        static CommandLineOption CDF3;
        static CommandLineOption CDF4;
        static CommandLineOption CDF5;
        static CommandLineOption COORDS;
        static CommandLineOption DIMENSION;
        static CommandLineOption EXCLUDE;
        static CommandLineOption FILE_FORMAT;
        static CommandLineOption FIX_RECORD_DIMENSION;
        static CommandLineOption HELP;
        static CommandLineOption HISTORY;
        static CommandLineOption INPUT_PATH;
        static CommandLineOption JOIN;
        static CommandLineOption LATLONBOX;
        static CommandLineOption NO_COORDS;
        static CommandLineOption OPERATION;
        static CommandLineOption OP_TYPE;
        static CommandLineOption OUTPUT;
        static CommandLineOption OVERWRITE;
        static CommandLineOption TOPOLOGY;
        static CommandLineOption UNION;
        static CommandLineOption VARIABLE;

    protected:
        int _value;
        vector<string> _names;
        bool _has_argument;
        string _description;
        int _count;
        vector<string> _arguments;
};

#endif // COMMANDLINEOPTION_H_
