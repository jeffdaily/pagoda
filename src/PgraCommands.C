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
#include "PgraCommands.H"
#include "Timing.H"

using std::find;


vector<string> PgraCommands::VALID;


PgraCommands::PgraCommands()
    :   GenericCommands()
    ,   op_type("")
{
    init();
}


PgraCommands::PgraCommands(int argc, char **argv)
    :   GenericCommands()
    ,   op_type("")
{
    init();
    parse(argc, argv);
}


PgraCommands::~PgraCommands()
{
}


void PgraCommands::parse(int argc, char **argv)
{
    GenericCommands::parse(argc,argv);

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
 * Creates Dataset, joining on the record dimension.
 */
Dataset* PgraCommands::get_dataset()
{
    Dataset *dataset;
    Aggregation *agg;

    if (input_filenames.empty()) {
        throw CommandException("Missing input files");
    }

    if (input_filenames.size() == 1) {
        dataset = Dataset::open(input_filenames[0]);
    }
    else {
        // we need to determine the record dimension
        Dataset *first_dataset = Dataset::open(input_filenames[0]);
        Dimension *udim = first_dataset->get_udim();
        if (udim == NULL) {
            throw CommandException(
                "first input file does not contain a record dimension");
        }
        join_name = udim->get_name();
        dataset = agg = new AggregationJoinExisting(join_name);
        agg->add(first_dataset);
        for (size_t i=1,limit=input_filenames.size(); i<limit; ++i) {
            agg->add(Dataset::open(input_filenames[i]));
        }
    }

    if (file_format == FF_UNKNOWN) {
        file_format = dataset->get_file_format();
    }

    if (fix_record_dimension && record_dimension_size < 0) {
        Dimension *udim = dataset->get_udim();
        if (udim) {
            record_dimension_size = udim->get_size();
        }
    }

    return dataset;
}


FileWriter* PgraCommands::get_output() const
{
    FileWriter *writer = GenericCommands::get_output();

    if (is_fixing_record_dimension()) {
        writer->fixed_record_dimension(1);
    }


    return writer;
}


string PgraCommands::get_operator() const
{
    return op_type;
}


void PgraCommands::init()
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
