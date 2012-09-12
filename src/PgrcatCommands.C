#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>

#include "Aggregation.H"
#include "AggregationJoinExisting.H"
#include "CommandException.H"
#include "Dataset.H"
#include "Dimension.H"
#include "Error.H"
#include "FileWriter.H"
#include "GenericCommands.H"
#include "PgrcatCommands.H"


PgrcatCommands::PgrcatCommands()
    :   GenericCommands()
{
    init();
}


PgrcatCommands::PgrcatCommands(int argc, char **argv)
    :   GenericCommands()
{
    init();
    parse(argc, argv);
}


PgrcatCommands::~PgrcatCommands()
{
}


/**
 * Creates Dataset, joining on the record dimension.
 */
Dataset* PgrcatCommands::get_dataset()
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
        string join_name;
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


void PgrcatCommands::init()
{
}
