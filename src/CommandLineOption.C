#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <getopt.h>
#include <unistd.h>

#include "CommandLineOption.H"


CommandLineOption CommandLineOption::HELP(
        'h', "help", false,
        "print this usage information and exit");
CommandLineOption CommandLineOption::INPUT_PATH(
        'p', "path", true,
        "path prefix for all input filenames");
CommandLineOption CommandLineOption::OUTPUT(
        'o', "output", true,
        "output file name (or use last positional argument)");
CommandLineOption CommandLineOption::VARIABLE(
        'v', "variable", true,
        "var1[,var2[,...]] variable(s) to process");
CommandLineOption CommandLineOption::EXCLUDE(
        'x', "exclude", false,
        "extract all variables EXCEPT those specified with -v");
CommandLineOption CommandLineOption::NO_COORDS(
        'C', "no-coords", false,
        "associated coordinate variables should not be processed");
CommandLineOption CommandLineOption::COORDS(
        'c', "coords", false,
        "all coordinate variables will be processed");
CommandLineOption CommandLineOption::TOPOLOGY(
        'T', "topology", false,
        "do not process toplogy variables");
CommandLineOption CommandLineOption::DIMENSION(
        'd', "dimension", true,
        "dim[,min[,max[,stride]]]");
CommandLineOption CommandLineOption::AUXILIARY(
        'X', "auxiliary", true,
        "lon_min,lon_max,lat_min,lat_max auxiliary coordinate bounding box");
CommandLineOption CommandLineOption::LATLONBOX(
        'b', "box", true,
        "north,south,east,west lat/lon bounding box");
CommandLineOption CommandLineOption::OPERATION(
        'y', "operation", true,
        "one of {avg,sqravg,avgsqr,max,min,rms,rmssdn,sqrt,ttl}");
CommandLineOption CommandLineOption::OVERWRITE(
        'O', "overwrite", false,
        "overwrite existing output file, if any");
CommandLineOption CommandLineOption::APPEND(
        'A', "append", false,
        "append to existing output file, if any");
CommandLineOption CommandLineOption::HISTORY(
        'h', "history", false,
        "do not append to 'history' global attribute");
CommandLineOption CommandLineOption::ALPHABETIZE(
        'a', "alphabetize", false,
        "disable alphabetization of extracted variables");
CommandLineOption CommandLineOption::JOIN(
        'j', "join", true,
        "dimension   join all input files on the given dimension");
CommandLineOption CommandLineOption::UNION(
        'u', "union", false,
        "dimension   take the union of all input files");


CommandLineOption::CommandLineOption(const int &value, const string &name,
        const bool &argument, const string &description)
    :   _value(value)
    ,   _name(name)
    ,   _has_argument(argument)
    ,   _description(description)
    ,   _count(0)
    ,   _arguments()
{
}


CommandLineOption::~CommandLineOption()
{
}


void inline CommandLineOption::set_value(const int &value)
{
    _value = value;
}


void inline CommandLineOption::set_name(const string &name)
{
    _name = name;
}


void inline CommandLineOption::has_argument(const bool &argument)
{
    _has_argument = argument;
}


void inline CommandLineOption::set_description(const string &description)
{
    _description = description;
}


int inline CommandLineOption::get_value() const
{
    return _value;
}


string inline CommandLineOption::get_name() const
{
    return _name;
}


bool inline CommandLineOption::has_argument() const
{
    return _has_argument;
}


string inline CommandLineOption::get_description() const
{
    return _description;
}


bool CommandLineOption::handle(int value, const string &arg, const string &name)
{
    if (_value == value || (0 == value && _name == name)) {
        ++_count;
        if (_has_argument) {
            _arguments.push_back(arg);
        }
        return true;
    }

    return false;
}


string CommandLineOption::get_usage() const
{
    // this needs fixing, see boost program options
    string left;
    if ((_value >= 'A' && _value <= 'Z')
            || (_value >= 'a' && _value <= 'z')
            || (_value >= '0' && _value <= '9')) {
        left += '-';
        left += char(_value);
        left += ",";
    }
    left += "--" + _name;

    return left + "\t" + _description;
}


string CommandLineOption::get_option() const
{
    string ret;
    ret += char(_value);
    if (_has_argument) {
        return ret + ':';
    }
    return ret;
}


struct option CommandLineOption::get_option_long() const
{
    struct option ret;

    ret.name = _name.c_str();
    ret.has_arg = _has_argument;
    ret.flag = NULL;
    ret.val = _value;

    return ret;
}


int inline CommandLineOption::get_count() const
{
    return _count;
}


string inline CommandLineOption::get_argument() const
{
    if (!_arguments.empty()) {
        return _arguments.front();
    }
    return "";
}


vector<string> inline CommandLineOption::get_arguments() const
{
    return _arguments;
}


struct option CommandLineOption::get_long_opt_null()
{
    struct option ret;

    ret.name = NULL;
    ret.has_arg = no_argument;
    ret.flag = NULL;
    ret.val = 0;

    return ret;
}
