#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#ifdef HAVE_UNISTD_H
#   include <unistd.h> // for getopt
#endif

#include "Aggregation.H"
#include "AggregationJoinExisting.H"
#include "AggregationUnion.H"
#include "CommandLineOption.H"
#include "CommandLineParser.H"
#include "Common.H"
#include "Dataset.H"
#include "Error.H"
#include "FileWriter.H"
#include "PagodaException.H"
#include "SubsetterCommands.H"
#include "Timing.H"


SubsetterCommands::SubsetterCommands()
    :   CommandLineParser()
    ,   input_filenames()
    ,   output_filename("")
    ,   join_name("")
    ,   slices()
    ,   _has_box(false)
    ,   box()
{
    TIMING("SubsetterCommands::SubsetterCommands()");
    init();
}


SubsetterCommands::SubsetterCommands(int argc, char **argv)
    :   input_filenames()
    ,   output_filename("")
    ,   join_name("")
    ,   slices()
    ,   _has_box(false)
    ,   box()
{
    TIMING("SubsetterCommands::SubsetterCommands(int,char**)");
    init();
    parse(argc, argv);
}


void SubsetterCommands::init()
{
    push_back(&CommandLineOption::OUTPUT);
    push_back(&CommandLineOption::LATLONBOX);
    push_back(&CommandLineOption::DIMENSION);
    push_back(&CommandLineOption::VARIABLE);
}


SubsetterCommands::~SubsetterCommands()
{
    TIMING("SubsetterCommands::~SubsetterCommands()");
}


Dataset* SubsetterCommands::get_dataset() const
{
    Dataset *dataset;
    Aggregation *agg;

    if (input_filenames.empty()) {
        ERR("Missing input files");
    }

    if (input_filenames.size() == 1) {
        dataset = Dataset::open(input_filenames[0]);
    } else {
        if (join_name.empty()) {
            dataset = agg = new AggregationUnion;
        } else {
            dataset = agg = new AggregationJoinExisting(join_name);
        }
        for (size_t i=0,limit=input_filenames.size(); i<limit; ++i) {
            agg->add(Dataset::open(input_filenames[i]));
        }
    }

    return dataset;
}


FileWriter* SubsetterCommands::get_output() const
{
    return FileWriter::create(output_filename);
}


void SubsetterCommands::parse(int argc, char **argv)
{
    TIMING("SubsetterCommands::parse(int,char**)");

    CommandLineParser::parse(argc,argv);

    if (positional_arguments.empty()) {
        ERR("input and output file arguments required");
    }

    if (count("output") == 0) {
        if (positional_arguments.size() == 1) {
            ERR("output file argument required");
        } else if (positional_arguments.size() == 0) {
            ERR("input and output file arguments required");
        }
        output_filename = positional_arguments.back();
        input_filenames.assign(positional_arguments.begin(),
                positional_arguments.end()-1);
    } else if (count("output") == 1) {
        output_filename = get_argument("output");
        input_filenames = positional_arguments;
    } else if (count("output") > 1) {
        ERR("too many output file arguments");
    }

    if (count("auxiliary") == 0) {
    } else if (count("auxiliary") == 1) {
        //box.set(get_argument("auxiliary"));
        box.set_auxiliary(get_argument("auxiliary"));
    } else {
        ERR("too many auxiliary coordinate boxes");
    }
}


LatLonBox SubsetterCommands::get_box() const
{
    TIMING("SubsetterCommands::get_box()");
    return box;
}


vector<DimSlice> SubsetterCommands::get_slices() const
{
    TIMING("SubsetterCommands::get_slices()");
    return slices;
}


vector<string> SubsetterCommands::get_input_filenames() const
{
    return input_filenames;
}


string SubsetterCommands::get_output_filename() const
{
    return output_filename;
}


string SubsetterCommands::get_join_name() const
{
    return join_name;
}
