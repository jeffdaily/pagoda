#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "CommandException.H"
#include "CommandLineOption.H"
#include "Dataset.H"
#include "Dimension.H"
#include "Error.H"
#include "GenericCommands.H"
#include "PgecatCommands.H"


PgecatCommands::PgecatCommands()
    :   GenericCommands()
    ,   unlimited_name("record")
{
    init();
}


PgecatCommands::PgecatCommands(int argc, char **argv)
    :   GenericCommands()
    ,   unlimited_name("record")
{
    init();
    parse(argc, argv);
}


PgecatCommands::~PgecatCommands()
{
}


void PgecatCommands::parse(int argc, char **argv)
{
    GenericCommands::parse(argc, argv);

    if (parser.count(CommandLineOption::UNLIMITED_DIMENSION_NAME)) {
        unlimited_name = 
            parser.get_argument(CommandLineOption::UNLIMITED_DIMENSION_NAME);
    }
}


/**
 * Not implemented.
 */
Dataset* PgecatCommands::get_dataset()
{
    ERR("Not Supported -- disabled from GenericCommands");
}


/**
 * Creates Datasets.
 *
 * All input files are separate Datasets.
 */
vector<Dataset*> PgecatCommands::get_datasets()
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
PgecatCommands::get_variables(const vector<Dataset*> &datasets)
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
PgecatCommands::get_dimensions(const vector<Dataset*> &datasets)
{
    vector<vector<Dimension*> > result;
    vector<Dataset*>::const_iterator it;
    vector<Dataset*>::const_iterator end=datasets.end();

    for (it=datasets.begin(); it!=end; ++it) {
        result.push_back(GenericCommands::get_dimensions(*it));
    }

    return result;
}


FileWriter* PgecatCommands::get_output() const
{
    FileWriter *writer = GenericCommands::get_output();

    return writer;
}


string PgecatCommands::get_unlimited_name() const
{
    return unlimited_name;
}


void PgecatCommands::init()
{
    parser.push_back(CommandLineOption::UNLIMITED_DIMENSION_NAME);
}
