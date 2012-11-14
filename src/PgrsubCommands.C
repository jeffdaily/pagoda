#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>

#include "Aggregation.H"
#include "AggregationJoinExisting.H"
#include "AggregationUnion.H"
#include "CommandException.H"
#include "Dataset.H"
#include "Dimension.H"
#include "FileWriter.H"
#include "GenericCommands.H"
#include "PgrsubCommands.H"
#include "Util.H"
#include "Variable.H"

using std::sort;


PgrsubCommands::PgrsubCommands()
    :   GenericCommands()
    ,   join_name("")
    ,   alphabetize(true)
    ,   dimension_masks()
{
    init();
}


PgrsubCommands::PgrsubCommands(int argc, char **argv)
    :   GenericCommands()
    ,   join_name("")
    ,   alphabetize(true)
    ,   dimension_masks()
{
    init();
    parse(argc, argv);
}


PgrsubCommands::~PgrsubCommands()
{
}


void PgrsubCommands::parse(int argc, char **argv)
{
    GenericCommands::parse(argc, argv);

    if (parser.count(CommandLineOption::JOIN)) {
        join_name = parser.get_argument(CommandLineOption::JOIN);
    }

    if (parser.count(CommandLineOption::UNION)) {
    }

    if (parser.count(CommandLineOption::ALPHABETIZE)) {
        alphabetize = false;
    }

    if (parser.count(CommandLineOption::DIMENSION_MASK)) {
        dimension_masks = parser.get_arguments(CommandLineOption::DIMENSION_MASK);
    }
}


/**
 * Creates and returns a new Dataset.
 *
 * Interprets the command-line parameters for union and join aggregations.
 * Determines the file format and record size and stores locally (otherwise
 * this method would be const like the rest).
 */
Dataset* PgrsubCommands::get_dataset()
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


static bool var_cmp(Variable *left, Variable *right)
{
    return left->get_name() < right->get_name();
}


vector<Variable*> PgrsubCommands::get_variables(Dataset *dataset)
{
    vector<Variable*> vars_out = GenericCommands::get_variables(dataset);

    if (alphabetize) {
        sort(vars_out.begin(), vars_out.end(), var_cmp);
    }

    variables_cache[dataset] = vars_out;

    return vars_out;

}


static bool dim_cmp(Dimension *left, Dimension *right)
{
    return left->get_name() < right->get_name();
}


vector<Dimension*> PgrsubCommands::get_dimensions(Dataset *dataset)
{
    vector<Dimension*> dims_out = GenericCommands::get_dimensions(dataset);

    if (alphabetize) {
        sort(dims_out.begin(), dims_out.end(), dim_cmp);
    }

    dimensions_cache[dataset] = dims_out;

    return dims_out;
}


vector<string> PgrsubCommands::get_dimension_masks(Dataset *dataset)
{
    return dimension_masks;
}


string PgrsubCommands::get_output_filename_with_index(int64_t index) const
{
    string filename = GenericCommands::get_output_filename();
    string str_index = pagoda::to_string(index);

    if (pagoda::ends_with(filename, ".nc")) {
        // filename already has .nc extension, so insert index
        filename.replace(filename.size()-4, 4, str_index + ".nc");
    }
    else {
        // filename does not have .nc extension, so append index
        filename.append(str_index + ".nc");
    }
    
    return filename;
}


FileWriter* PgrsubCommands::get_output_at(int64_t index) const
{
    string filename = get_output_filename_with_index(index);
    FileWriter *writer = FileWriter::open(filename, file_format);

    writer->append(append)
        ->overwrite(overwrite)
        ->fixed_record_dimension(record_dimension_size)
        ->header_pad(header_pad)
        ->create();

    return writer;
}


string PgrsubCommands::get_join_name() const
{
    return join_name;
}


bool PgrsubCommands::is_alphabetizing() const
{
    return alphabetize;
}


void PgrsubCommands::init()
{
    parser.push_back(CommandLineOption::ALPHABETIZE);
    parser.push_back(CommandLineOption::DIMENSION_MASK);
    parser.push_back(CommandLineOption::JOIN);
    parser.push_back(CommandLineOption::UNION);
}
