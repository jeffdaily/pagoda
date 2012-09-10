#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Aggregation.H"
#include "AggregationJoinExisting.H"
#include "AggregationUnion.H"
#include "CommandException.H"
#include "Dataset.H"
#include "Dimension.H"
#include "GenericCommands.H"
#include "SubsetterCommands.H"


SubsetterCommands::SubsetterCommands()
    :   GenericCommands()
    ,   join_name("")
{
    init();
}


SubsetterCommands::SubsetterCommands(int argc, char **argv)
    :   GenericCommands()
    ,   join_name("")
{
    init();
    parse(argc, argv);
}


SubsetterCommands::~SubsetterCommands()
{
}


void SubsetterCommands::parse(int argc, char **argv)
{
    GenericCommands::parse(argc, argv);

    if (parser.count(CommandLineOption::JOIN)) {
        join_name = parser.get_argument(CommandLineOption::JOIN);
    }

    if (parser.count(CommandLineOption::UNION)) {
    }
}


/**
 * Creates and returns a new Dataset.
 *
 * Interprets the command-line parameters for union and join aggregations.
 * Determines the file format and record size and stores locally (otherwise
 * this method would be const like the rest).
 */
Dataset* SubsetterCommands::get_dataset()
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
        if (join_name.empty()) {
            dataset = agg = new AggregationUnion;
        }
        else {
            dataset = agg = new AggregationJoinExisting(join_name);
        }
        for (size_t i=0,limit=input_filenames.size(); i<limit; ++i) {
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


string SubsetterCommands::get_join_name() const
{
    return join_name;
}


void SubsetterCommands::init()
{
    parser.push_back(CommandLineOption::JOIN);
    parser.push_back(CommandLineOption::UNION);
}
