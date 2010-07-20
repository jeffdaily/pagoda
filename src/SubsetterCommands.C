#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#ifdef HAVE_UNISTD_H
#   include <unistd.h> // for getopt
#endif

#include <algorithm>

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
    :   parser()
    ,   input_filenames()
    ,   output_filename("")
    ,   variables()
    ,   exclude(false)
    ,   join_name("")
    ,   slices()
    ,   boxes()
{
    TIMING("SubsetterCommands::SubsetterCommands()");
    init();
}


SubsetterCommands::SubsetterCommands(int argc, char **argv)
    :   input_filenames()
    ,   output_filename("")
    ,   variables()
    ,   exclude(false)
    ,   join_name("")
    ,   slices()
    ,   boxes()
{
    TIMING("SubsetterCommands::SubsetterCommands(int,char**)");
    init();
    parse(argc, argv);
}


void SubsetterCommands::init()
{
    parser.push_back(&CommandLineOption::OUTPUT);
    parser.push_back(&CommandLineOption::AUXILIARY);
    parser.push_back(&CommandLineOption::LATLONBOX);
    parser.push_back(&CommandLineOption::DIMENSION);
    parser.push_back(&CommandLineOption::VARIABLE);
    parser.push_back(&CommandLineOption::EXCLUDE);
    parser.push_back(&CommandLineOption::JOIN);
    parser.push_back(&CommandLineOption::UNION);
}


SubsetterCommands::~SubsetterCommands()
{
    TIMING("SubsetterCommands::~SubsetterCommands()");
}


void SubsetterCommands::parse(int argc, char **argv)
{
    vector<string> positional_arguments;

    TIMING("SubsetterCommands::parse(int,char**)");

    parser.parse(argc,argv);
    positional_arguments = parser.get_positional_arguments();

    if (positional_arguments.empty()) {
        ERR("input and output file arguments required");
    }

    if (parser.count("output") == 0) {
        if (positional_arguments.size() == 1) {
            ERR("output file argument required");
        } else if (positional_arguments.size() == 0) {
            ERR("input and output file arguments required");
        }
        output_filename = positional_arguments.back();
        input_filenames.assign(positional_arguments.begin(),
                positional_arguments.end()-1);
    } else if (parser.count("output") == 1) {
        output_filename = parser.get_argument("output");
        input_filenames = positional_arguments;
    } else if (parser.count("output") > 1) {
        ERR("too many output file arguments");
    }

    if (parser.count("auxiliary")) {
        vector<string> args = parser.get_arguments("auxiliary");
        for (vector<string>::iterator it=args.begin(); it!=args.end(); ++it) {
            boxes.push_back(LatLonBox(*it, true));
        }
    }

    if (parser.count("box")) {
        vector<string> args = parser.get_arguments("box");
        for (vector<string>::iterator it=args.begin(); it!=args.end(); ++it) {
            boxes.push_back(LatLonBox(*it, false));
        }
    }

    if (parser.count("exclude")) {
        exclude = true;
    }

    if (parser.count("variable")) {
        vector<string> args = parser.get_arguments("variable");
        vector<string>::iterator truncate_location;
        for (vector<string>::iterator it=args.begin(); it!=args.end(); ++it) {
            istringstream iss(*it);
            while (iss) {
                string value;
                getline(iss, value, ',');
                if (!value.empty()) {
                        variables.push_back(value);
                }
            }
        }
        // sort and remove duplicates
        std::sort(variables.begin(), variables.end());
        truncate_location = std::unique(variables.begin(), variables.end());
        variables.resize(truncate_location - variables.begin());
    }

    if (parser.count("dimension")) {
        vector<string> args = parser.get_arguments("dimension");
        for (vector<string>::iterator it=args.begin(); it!=args.end(); ++it) {
            slices.push_back(DimSlice(*it));
        }
    }

    if (parser.count("join")) {
        join_name = parser.get_argument("join");
    }
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


vector<LatLonBox> SubsetterCommands::get_boxes() const
{
    TIMING("SubsetterCommands::get_box()");
    return boxes;
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


vector<string> SubsetterCommands::get_variables() const
{
    return variables;
}


bool SubsetterCommands::get_exclude() const
{
    return exclude;
}


string SubsetterCommands::get_join_name() const
{
    return join_name;
}
