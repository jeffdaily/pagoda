#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>

#include "CommandException.H"
#include "CommandLineOption.H"
#include "Dataset.H"
#include "Error.H"
#include "GenericCommands.H"
#include "PgboCommands.H"
#include "Timing.H"

using std::find;


vector<string> PgboCommands::ADD;
vector<string> PgboCommands::SUB;
vector<string> PgboCommands::MUL;
vector<string> PgboCommands::DIV;



PgboCommands::PgboCommands()
    :   GenericCommands()
    ,   op_type("")
{
    TIMING("PgboCommands::PgboCommands()");
    init();
}


PgboCommands::PgboCommands(int argc, char **argv)
    :   GenericCommands()
    ,   op_type("")
{
    TIMING("PgboCommands::PgboCommands(int,char**)");
    init();
    parse(argc, argv);
}


PgboCommands::~PgboCommands()
{
    TIMING("PgboCommands::~PgboCommands()");
}


void PgboCommands::parse(int argc, char **argv)
{
    GenericCommands::parse(argc,argv);

    if (is_helping()) {
        return;
    }

    if (input_filenames.size() != 2) {
        throw CommandException("two and only input files required");
    }

    if (parser.count("op_typ")) {
        vector<string> valid;
        valid.insert(valid.end(), ADD.begin(), ADD.end());
        valid.insert(valid.end(), SUB.begin(), SUB.end());
        valid.insert(valid.end(), MUL.begin(), MUL.end());
        valid.insert(valid.end(), DIV.begin(), DIV.end());
        op_type = parser.get_argument("op_typ");
        if (find(valid.begin(),valid.end(),op_type) == valid.end()) {
            throw CommandException("operator '" + op_type + "' not recognized");
        }
    }
    else {
        throw CommandException("operator is required");
    }
}


Dataset* PgboCommands::get_dataset() const
{
    if (input_filenames.size() != 2) {
        throw CommandException("two and only input files required");
    }
    return Dataset::open(input_filenames[0]);
}


Dataset* PgboCommands::get_operand() const
{
    if (input_filenames.size() != 2) {
        throw CommandException("two and only input files required");
    }
    return Dataset::open(input_filenames[1]);
}


string PgboCommands::get_operator() const
{
    // normalize the possible results
    if (find(ADD.begin(),ADD.end(),op_type) != ADD.end()) {
        return OP_ADD;
    }
    else if (find(SUB.begin(),SUB.end(),op_type) != SUB.end()) {
        return OP_SUB;
    }
    else if (find(MUL.begin(),MUL.end(),op_type) != MUL.end()) {
        return OP_MUL;
    }
    else if (find(DIV.begin(),DIV.end(),op_type) != DIV.end()) {
        return OP_DIV;
    }
    else {
        throw CommandException("could not normalize operator");
    }
}


void PgboCommands::init()
{
    parser.push_back(&CommandLineOption::OP_TYPE);
    // erase the aggregation ops
    parser.erase(&CommandLineOption::JOIN);
    parser.erase(&CommandLineOption::UNION);

    if (ADD.empty()) {
        ADD.push_back("add");
        ADD.push_back("+");
        ADD.push_back("addition");
    }
    if (SUB.empty()) {
        SUB.push_back("sbt");
        SUB.push_back("-");
        SUB.push_back("dff");
        SUB.push_back("diff");
        SUB.push_back("sub");
        SUB.push_back("subtract");
        SUB.push_back("subtraction");
    }
    if (MUL.empty()) {
        MUL.push_back("mlt");
        MUL.push_back("*");
        MUL.push_back("mult");
        MUL.push_back("multiply");
        MUL.push_back("multiplication");
    }
    if (DIV.empty()) {
        DIV.push_back("dvd");
        DIV.push_back("/");
        DIV.push_back("divide");
        DIV.push_back("division");
    }
}
