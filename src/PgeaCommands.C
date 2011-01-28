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
#include "PgeaCommands.H"
#include "Timing.H"

using std::find;


vector<string> PgeaCommands::VALID;


PgeaCommands::PgeaCommands()
    :   GenericCommands()
    ,   op_type("")
{
    TIMING("PgeaCommands::PgeaCommands()");
    init();
}


PgeaCommands::PgeaCommands(int argc, char **argv)
    :   GenericCommands()
    ,   op_type("")
{
    TIMING("PgeaCommands::PgeaCommands(int,char**)");
    init();
    parse(argc, argv);
}


PgeaCommands::~PgeaCommands()
{
    TIMING("PgeaCommands::~PgeaCommands()");
}


void PgeaCommands::parse(int argc, char **argv)
{
    GenericCommands::parse(argc,argv);

    if (is_requesting_info()) {
        return;
    }

    if (parser.count("op_typ")) {
        op_type = parser.get_argument("op_typ");
        if (find(VALID.begin(),VALID.end(),op_type) == VALID.end()) {
            throw CommandException("operator '" + op_type + "' not recognized");
        }
    }
    else {
        op_type = OP_AVG;
    }
}


/**
 * Not implemented.
 */
Dataset* PgeaCommands::get_dataset()
{
    ERR("Not Supported -- disabled from GenericCommands");
}


/**
 * Creates Datasets.
 *
 * All input files are separate Datasets.
 */
vector<Dataset*> PgeaCommands::get_datasets()
{
    vector<Dataset*> datasets;

    if (input_filenames.empty()) {
        throw CommandException("Missing input files");
    }

    for (size_t i=0; i<input_filenames.size(); i++) {
        datasets.push_back(Dataset::open(input_filenames[i]));
    }

    if (file_format == FF_UNKNOWN) {
        file_format = datasets.at(0)->get_file_format();
    }

    if (fix_record_dimension && record_dimension_size < 0) {
        Dimension *udim = datasets.at(0)->get_udim();
        if (udim) {
            record_dimension_size = udim->get_size();
        }
    }

    return datasets;
}


vector<vector<Variable*> >
PgeaCommands::get_variables(const vector<Dataset*> &datasets)
{
    vector<vector<Variable*> > result;
    vector<Dataset*>::const_iterator it;
    vector<Dataset*>::const_iterator end=datasets.end();

    for (it=datasets.begin(); it!=end; ++it) {
        result.push_back(GenericCommands::get_variables(*it));
    }

    return result;
}


vector<vector<Dimension*> >
PgeaCommands::get_dimensions(const vector<Dataset*> &datasets)
{
    vector<vector<Dimension*> > result;
    vector<Dataset*>::const_iterator it;
    vector<Dataset*>::const_iterator end=datasets.end();

    for (it=datasets.begin(); it!=end; ++it) {
        result.push_back(GenericCommands::get_dimensions(*it));
    }

    return result;
}


FileWriter* PgeaCommands::get_output() const
{
    FileWriter *writer = GenericCommands::get_output();

    if (is_fixing_record_dimension()) {
        writer->fixed_record_dimension(1);
    }

    return writer;
}


string PgeaCommands::get_operator() const
{
    return op_type;
}


void PgeaCommands::init()
{
    parser.push_back(&CommandLineOption::AVG_TYPE);
    // erase the aggregation ops
    parser.erase(&CommandLineOption::JOIN);
    parser.erase(&CommandLineOption::UNION);

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
