#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <getopt.h>
#include <unistd.h>

#include <cassert>

#include "CommandLineOption.H"
#include "Util.H"


unsigned int CommandLineOption::WIDTH(20);

CommandLineOption CommandLineOption::ALPHABETIZE(
    'a', "alphabetize", false,
    "disable alphabetization of extracted variables");
CommandLineOption CommandLineOption::APPEND(
    'A', "append", false,
    "append to existing output file, if any");
CommandLineOption CommandLineOption::AUXILIARY(
    'X', "auxiliary", true,
    "lon_min,lon_max,lat_min,lat_max auxiliary coordinate bounding box");
CommandLineOption CommandLineOption::AVG_TYPE(
    'y', "op_typ", true,
    "average operation: avg,sqravg,avgsqr,max,min,rms,rmssdn,sqrt,ttl");
CommandLineOption CommandLineOption::CB_BUFFER_SIZE(
    0, "cb_buffer_size", true,
    "buffer size for collective IO (in bytes)");
CommandLineOption CommandLineOption::CDF1(
    '3', "3", false,
    "output file in netCDF3 CLASSIC (32-bit offset) storage format");
CommandLineOption CommandLineOption::CDF2(
    '4', "64bit", false,
    "output file in netCDF3 64-bit offset storage format");
CommandLineOption CommandLineOption::CDF5(
    '5', "5", false,
    "output file in parallel netCDF 64-bit data format");
CommandLineOption CommandLineOption::COORDS(
    'c', "coords", false,
    "all coordinate variables will be processed",
    "crd");
CommandLineOption CommandLineOption::DIMENSION(
    'd', "dimension", true,
    "dim[,min[,max[,stride]]]",
    "dmn");
CommandLineOption CommandLineOption::EXCLUDE(
    'x', "exclude", false,
    "extract all variables EXCEPT those specified with -v",
    "xcl");
CommandLineOption CommandLineOption::FILE_FORMAT(
    0, "file_format", true,
    "=classic same as -3* =64bit same as -6* =64data same as -5",
    "fl_fmt");
CommandLineOption CommandLineOption::FIX_RECORD_DIMENSION(
    0, "fix_rec_dmn", false,
    "change record dimension into fixed dimension in output file");
CommandLineOption CommandLineOption::GROUPS(
    0, "groups", true,
    "use indicated number of process groups for IO (default: 1)");
CommandLineOption CommandLineOption::HEADER_PAD(
    0, "header_pad", true,
    "pad output header with indicated number of bytes",
    "hdr_pad");
CommandLineOption CommandLineOption::HELP(
    'h', "help", false,
    "print this usage information and exit");
CommandLineOption CommandLineOption::HISTORY(
    'h', "history", false,
    "do not append to 'history' global attribute",
    "hst");
CommandLineOption CommandLineOption::INPUT_PATH(
    'p', "path", true,
    "path prefix for all input filenames",
    "pth");
CommandLineOption CommandLineOption::INTERPOLATE(
    'i', "interpolate", true,
    "interpolant and value",
    "ntp");
CommandLineOption CommandLineOption::JOIN(
    'j', "join", true,
    "join all input files on the given dimension");
CommandLineOption CommandLineOption::LATLONBOX(
    'b', "box", true,
    "north,south,east,west lat/lon bounding box");
CommandLineOption CommandLineOption::NC4(
    0, "netcdf4", false,
    "output file in netCDF4 data format");
CommandLineOption CommandLineOption::NC4_CLASSIC(
    0, "netcdf4_classic", false,
    "output file in netCDF4 classic data format");
CommandLineOption CommandLineOption::NO_COORDS(
    'C', "no-coords", false,
    "associated coordinate variables should not be processed");
CommandLineOption CommandLineOption::NONBLOCKING_IO(
    0, "nbio", false,
    "use non-blocking IO, if possible");
CommandLineOption CommandLineOption::OPERATION(
    'y', "operation", true,
    "one of {avg,sqravg,avgsqr,max,min,rms,rmssdn,sqrt,ttl}");
CommandLineOption CommandLineOption::OP_TYPE(
    'y', "op_typ", true,
    "binary arithmetic operation: add,sbt,mlt,dvd (+,-,*,/)");
CommandLineOption CommandLineOption::OUTPUT(
    'o', "output", true,
    "output file name (or use last positional argument)",
    "fl_out");
CommandLineOption CommandLineOption::OVERWRITE(
    'O', "overwrite", false,
    "overwrite existing output file, if any",
    "ovr");
CommandLineOption CommandLineOption::READ_ALL_RECORDS(
    0, "allrec", false,
    "read all records per variable rather than record-at-a-time");
CommandLineOption CommandLineOption::READ_ALL_VARIABLES(
    0, "allvar", false,
    "read all variables per file rather than variable-at-a-time");
CommandLineOption CommandLineOption::ROMIO_CB_READ(
    0, "romio_cb_read", true,
    "enable/disable/automatic collective buffering during read operations");
CommandLineOption CommandLineOption::ROMIO_DS_READ(
    0, "romio_ds_read", true,
    "enable/disable/automatic data sieving during read operations");
CommandLineOption CommandLineOption::ROMIO_NO_INDEP_RW(
    0, "romio_no_indep_rw", true,
    "enable/disable independent (vs collective) IO hint");
CommandLineOption CommandLineOption::STRIPING_UNIT(
    0, "striping_unit", true,
    "striping unit (in bytes)");
CommandLineOption CommandLineOption::TOPOLOGY(
    'T', "topology", false,
    "do not process toplogy variables");
CommandLineOption CommandLineOption::UNION(
    'u', "union", false,
    "take the union of all input files");
CommandLineOption CommandLineOption::VARIABLE(
    'v', "variable", true,
    "var1[,var2[,...]] variable(s) to process");
CommandLineOption CommandLineOption::VERS(
    'V', "version", false,
    "print version information and exit");
CommandLineOption CommandLineOption::WEIGHT(
    'w', "weight", true,
    "weight(s) of files",
    "wgt_var");


CommandLineOption::CommandLineOption(const int &value, const string &name,
                                     const bool &argument, const string &description,
                                     const string &extra_name1, const string &extra_name2)
    :   _value(value)
    ,   _names()
    ,   _has_argument(argument)
    ,   _description(description)
    ,   _count(0)
    ,   _arguments()
{
    assert(!name.empty());
    _names.push_back(name);
    if (!extra_name1.empty()) {
        _names.push_back(extra_name1);
    }
    if (!extra_name2.empty()) {
        _names.push_back(extra_name2);
    }
}


CommandLineOption::~CommandLineOption()
{
}


void CommandLineOption::set_value(const int &value)
{
    _value = value;
}


void CommandLineOption::set_name(const string &name)
{
    _names.clear();
    _names.push_back(name);
}


void CommandLineOption::set_names(const vector<string> &names)
{
    _names = names;
}


void CommandLineOption::has_argument(const bool &argument)
{
    _has_argument = argument;
}


void CommandLineOption::set_description(const string &description)
{
    _description = description;
}


int CommandLineOption::get_value() const
{
    return _value;
}


string CommandLineOption::get_name() const
{
    return _names.front();
}


vector<string> CommandLineOption::get_names() const
{
    return _names;
}


bool CommandLineOption::has_argument() const
{
    return _has_argument;
}


string CommandLineOption::get_description() const
{
    return _description;
}


bool CommandLineOption::handle(int value, const string &arg, const string &name)
{
    if ((0 != value && _value == value)
            || (0 == value
                && find(_names.begin(), _names.end(), name) != _names.end())) {
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
    string space(WIDTH, ' ');
    string usage;
    vector<string> parts = pagoda::split(_description);
    int length = 0;
    if ((_value >= 'A' && _value <= 'Z')
            || (_value >= 'a' && _value <= 'z')
            || (_value >= '0' && _value <= '9')) {
        usage += '-';
        usage += char(_value);
        usage += ", ";
    }
    usage += "--" + _names.front();
    for (vector<string>::const_iterator it=_names.begin()+1;
            it!=_names.end(); ++it) {
        usage += ", --" + *it;
    }

    if (usage.size() >= WIDTH) {
        usage += "\n";
        usage += space;
    }
    else {
        while (usage.size() < WIDTH) {
            usage += " ";
        }
    }

    for (size_t i=0,limit=parts.size(); i<limit; ++i) {
        assert(!parts[i].empty());
        if ((parts[i].size() + length) > (80 - WIDTH)) {
            usage += "\n";
            usage += space;
            length = 0;
        }
        if (parts[i][parts[i].size()-1] == '*') {
            usage += " ";
            usage += parts[i].substr(0, parts[i].size()-1);
            length += 1 + parts[i].size()-1;
            usage += "\n";
            usage += space;
            length = 0;
        }
        else {
            usage += " ";
            usage += parts[i];
            length += 1 + parts[i].size();
        }
    }

    return usage;
}


string CommandLineOption::get_option() const
{
    string ret;
    if (_value != 0) {
        ret += char(_value);
        if (_has_argument) {
            return ret + ':';
        }
    }
    return ret;
}


struct option CommandLineOption::get_long_option() const {
    struct option ret;

    ret.name = _names.front().c_str();
    ret.has_arg = _has_argument;
    ret.flag = NULL;
    ret.val = _value;

    return ret;
}


vector<struct option> CommandLineOption::get_long_options() const
{
    vector<struct option> ret;

    for (vector<string>::const_iterator it=_names.begin();
            it!=_names.end(); ++it) {
        struct option current_option;
        current_option.name = it->c_str();
        current_option.has_arg = _has_argument;
        current_option.flag = NULL;
        current_option.val = _value;
        ret.push_back(current_option);
    }

    return ret;
}


int CommandLineOption::get_count() const
{
    return _count;
}


string CommandLineOption::get_argument() const
{
    if (!_arguments.empty()) {
        return _arguments.back();
    }
    return "";
}


vector<string> CommandLineOption::get_arguments() const
{
    return _arguments;
}


struct option CommandLineOption::get_long_opt_null() {
    struct option ret;

    ret.name = NULL;
    ret.has_arg = no_argument;
    ret.flag = NULL;
    ret.val = 0;

    return ret;
}


ostream& operator<<(ostream &os, const CommandLineOption &op)
{
    vector<struct option> long_opts = op.get_long_options();
    vector<struct option>::iterator it=long_opts.begin();

    assert(!long_opts.empty());
    os << it->name << ", ";
    os << it->has_arg << ", ";
    os << it->flag << ", ";
    os << it->val;
    ++it;
    for (; it!=long_opts.end(); ++it) {
        os << endl;
        os << it->name << ", ";
        os << it->has_arg << ", ";
        os << it->flag << ", ";
        os << it->val;
    }

    return os;
}


ostream& operator<<(ostream &os, const struct option &op)
{
    os << op.name << ", ";
    os << op.has_arg << ", ";
    os << op.flag << ", ";
    os << op.val;
    return os;
}
