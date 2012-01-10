#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>

#include "Aggregation.H"
#include "AggregationJoinExisting.H"
#include "CommandException.H"
#include "CommandLineOption.H"
#include "Dataset.H"
#include "Dimension.H"
#include "Error.H"
#include "FileWriter.H"
#include "GenericCommands.H"
#include "PgwaCommands.H"
#include "Util.H"

using std::find;


vector<string> PgwaCommands::VALID;


PgwaCommands::PgwaCommands()
    :   GenericCommands()
    ,   op_type("")
    ,   mask_value("")
    ,   mask_variable("")
    ,   mask_comparator("")
    ,   dimensions()
{
    init();
}


PgwaCommands::PgwaCommands(int argc, char **argv)
    :   GenericCommands()
    ,   op_type("")
    ,   mask_value("")
    ,   mask_variable("")
    ,   mask_comparator("")
{
    init();
    parse(argc, argv);
}


PgwaCommands::~PgwaCommands()
{
}


void PgwaCommands::parse(int argc, char **argv)
{
    GenericCommands::parse(argc,argv);

    if (parser.count(CommandLineOption::AVERAGE_OPERATION)) {
        op_type = parser.get_argument(CommandLineOption::AVERAGE_OPERATION);
        if (find(VALID.begin(),VALID.end(),op_type) == VALID.end()) {
            throw CommandException("operator '" + op_type + "' not recognized");
        }
    }
    else {
        op_type = OP_AVG;
    }

    if (parser.count(CommandLineOption::MASK_CONDITION)) {
        // mask conditions are of the form "ORO < 1"
        vector<string> pieces = pagoda::split(
                parser.get_argument(CommandLineOption::MASK_CONDITION));
        if (pieces.size() != 3) {
            throw CommandException("invalid mask condition");
        }
        mask_variable   = pieces.at(0);
        mask_comparator = pieces.at(1);
        mask_value      = pieces.at(2);
    }
    else {
        mask_value = "1.0";
        if (parser.count(CommandLineOption::MASK_COMPARATOR)) {
            mask_comparator = parser.get_argument(CommandLineOption::MASK_COMPARATOR);
        }
        if (parser.count(CommandLineOption::MASK_NAME)) {
            mask_variable = parser.get_argument(CommandLineOption::MASK_NAME);
        }
        if (parser.count(CommandLineOption::MASK_VALUE)) {
            mask_value = parser.get_argument(CommandLineOption::MASK_VALUE);
        }
    }

    if (parser.count(CommandLineOption::AVERAGE_DIMENSIONS)) {
        vector<string> args =
            parser.get_arguments(CommandLineOption::AVERAGE_DIMENSIONS);
        for (vector<string>::iterator it=args.begin(); it!=args.end(); ++it) {
            istringstream iss(*it);
            while (iss) {
                string value;
                getline(iss, value, ',');
                if (!value.empty()) {
                    dimensions.insert(value);
                }
            }
        }
    }
}


string PgwaCommands::get_operator() const
{
    return op_type;
}


void PgwaCommands::init()
{
    // alphabetize 'a' is replaced with average dimensions 'a'
    //parser.erase(CommandLineOption::ALPHABETIZE);
    parser.push_back(CommandLineOption::AVERAGE_OPERATION);
    parser.push_back(CommandLineOption::AVERAGE_DIMENSIONS);
    parser.push_back(CommandLineOption::MASK_CONDITION);
    parser.push_back(CommandLineOption::MASK_COMPARATOR);
    parser.push_back(CommandLineOption::MASK_NAME);
    parser.push_back(CommandLineOption::MASK_VALUE);
    parser.push_back(CommandLineOption::WEIGHT_MASK_COORDINATE_VARIABLES);
    parser.push_back(CommandLineOption::NO_NORMALIZATION);
    parser.push_back(CommandLineOption::RETAIN_DEGENERATE_DIMENSIONS);
    // erase the aggregation ops
    //parser.erase(CommandLineOption::JOIN);
    //parser.erase(CommandLineOption::UNION);

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
