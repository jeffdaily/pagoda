#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>

#include "CommandLineOption.H"
#include "Error.H"
#include "GenericCommands.H"
#include "PgraCommands.H"
#include "Timing.H"

using std::find;


vector<string> PgraCommands::VALID;


PgraCommands::PgraCommands()
    :   GenericCommands()
    ,   op_type("")
{
    TIMING("PgraCommands::PgraCommands()");
    init();
}


PgraCommands::PgraCommands(int argc, char **argv)
    :   GenericCommands()
    ,   op_type("")
{
    TIMING("PgraCommands::PgraCommands(int,char**)");
    init();
    parse(argc, argv);
}


PgraCommands::~PgraCommands()
{
    TIMING("PgraCommands::~PgraCommands()");
}


void PgraCommands::parse(int argc, char **argv)
{
    GenericCommands::parse(argc,argv);

    if (get_help()) {
        return;
    }

    if (parser.count("op_typ")) {
        op_type = parser.get_argument("op_typ");
        if (find(VALID.begin(),VALID.end(),op_type) == VALID.end()) {
            ERR("operator '" + op_type + "' not recognized");
        }
    } else {
        op_type = OP_AVG;
    }
}


string PgraCommands::get_operator() const
{
    return op_type;
}


void PgraCommands::init()
{
    parser.push_back(&CommandLineOption::OP_TYPE);

    if (VALID.empty()) {
        VALID.push_back(OP_AVG);
        VALID.push_back(OP_SQRAVG);
        VALID.push_back(OP_AVGSQR);
        VALID.push_back(OP_MAX);
        VALID.push_back(OP_MIN);
        VALID.push_back(OP_RMS);
        VALID.push_back(OP_RMSSDN);
        VALID.push_back(OP_SQRT);
        VALID.push_back(OP_TTL);
    }
}
