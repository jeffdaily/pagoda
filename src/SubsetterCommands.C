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
#include "GenericCommands.H"
#include "SubsetterCommands.H"
#include "Variable.H"

using std::sort;


SubsetterCommands::SubsetterCommands()
    :   GenericCommands()
    ,   join_name("")
    ,   alphabetize(true)
{
    init();
}


SubsetterCommands::SubsetterCommands(int argc, char **argv)
    :   GenericCommands()
    ,   join_name("")
    ,   alphabetize(true)
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

    if (parser.count(CommandLineOption::ALPHABETIZE)) {
        alphabetize = false;
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


static bool var_cmp(Variable *left, Variable *right)
{
    return left->get_name() < right->get_name();
}


vector<Variable*> SubsetterCommands::get_variables(Dataset *dataset)
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


vector<Dimension*> SubsetterCommands::get_dimensions(Dataset *dataset)
{
    vector<Dimension*> dims_out = GenericCommands::get_dimensions(dataset);

    if (alphabetize) {
        sort(dims_out.begin(), dims_out.end(), dim_cmp);
    }

    dimensions_cache[dataset] = dims_out;

    return dims_out;
}


string SubsetterCommands::get_join_name() const
{
    return join_name;
}


bool SubsetterCommands::is_alphabetizing() const
{
    return alphabetize;
}


void SubsetterCommands::init()
{
    parser.push_back(CommandLineOption::ALPHABETIZE);
    parser.push_back(CommandLineOption::JOIN);
    parser.push_back(CommandLineOption::UNION);
}
