#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <getopt.h>
#include <unistd.h>

#include "CommandLineOption.H"
#include "CommandLineParser.H"
#include "Error.H"


CommandLineParser::CommandLineParser()
    :   options()
    ,   options_map()
    ,   positional_arguments()
{
}


CommandLineParser::~CommandLineParser()
{
}


void CommandLineParser::push_back(CommandLineOption *option)
{
    options.push_back(option);
    if (!option->get_name().empty()) {
        options_map[option->get_name()] = option;
    }
}


void CommandLineParser::parse(int argc, char **argv)
{
    int opt;
    string opt_string;
    vector<struct option> long_opts;
    int long_index;
    optvec_t::const_iterator it;

    // build the opt_string and long_opts
    opt_string.reserve(options.size()*2);
    long_opts.reserve(options.size());
    for (it=options.begin(); it!=options.end(); ++it) {
        opt_string += (*it)->get_option();
        long_opts.push_back((*it)->get_option_long());
    }

    // do the parsing
    while ((opt = getopt_long(argc, argv, opt_string.c_str(),
                    &long_opts[0], &long_index)) != -1) {
        for (it=options.begin(); it!=options.end(); ++it) {
            if (opt == 0) {
                if ((*it)->handle(opt, optarg, long_opts[long_index].name)) {
                    break;
                }
            } else {
                if ((*it)->handle(opt, optarg)) {
                    break;
                }
            }
        }
    }
    
    // the positional arguments are left, put them in a vector<string>
    while (optind < argc) {
        positional_arguments.push_back(argv[optind++]);
    }
}


int CommandLineParser::count(const string &name) const
{
    optmap_t::const_iterator it = options_map.find(name);
    if (it != options_map.end()) {
        return it->second->get_count();
    }
    return 0;
}


string CommandLineParser::get_argument(const string &name) const
{
    optmap_t::const_iterator it = options_map.find(name);
    if (it != options_map.end()) {
        return it->second->get_argument();
    }
    return "";
}


vector<string> CommandLineParser::get_arguments(const string &name) const
{
    optmap_t::const_iterator it = options_map.find(name);
    if (it != options_map.end()) {
        return it->second->get_arguments();
    }
    return vector<string>();
}


string CommandLineParser::get_usage() const
{
    string ret;
    for (optvec_t::const_iterator it=options.begin(); it!=options.end(); ++it) {
        ret += (*it)->get_usage() + "\n";
    }
    return ret;
}
