#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>

#include "Aggregation.H"
#include "AggregationJoinExisting.H"
#include "AggregationUnion.H"
#include "Attribute.H"
#include "CommandException.H"
#include "CommandLineOption.H"
#include "CommandLineParser.H"
#include "Common.H"
#include "Dataset.H"
#include "Dimension.H"
#include "Error.H"
#include "FileWriter.H"
#include "GenericAttribute.H"
#include "GenericCommands.H"
#include "Grid.H"
#include "PagodaException.H"
#include "Timing.H"
#include "TypedValues.H"
#include "Util.H"
#include "Variable.H"

using std::sort;


GenericCommands::GenericCommands()
    :   parser()
    ,   cmdline("")
    ,   input_filenames()
    ,   input_path("")
    ,   output_filename("")
    ,   variables()
    ,   variables_cache()
    ,   dimensions_cache()
    ,   exclude_variables(false)
    ,   join_name("")
    ,   alphabetize(true)
    ,   process_all_coords(false)
    ,   process_coords(true)
    ,   modify_history(true)
    ,   process_topology(true)
    ,   help(false)
    ,   append(false)
    ,   overwrite(false)
    ,   fix_record_dimension(false)
    ,   record_dimension_size(-1)
    ,   header_pad(-1)
    ,   slices()
    ,   boxes()
    ,   file_format(FF_UNKNOWN)
{
    TIMING("GenericCommands::GenericCommands()");
    init();
}


GenericCommands::GenericCommands(int argc, char **argv)
    :   parser()
    ,   cmdline("")
    ,   input_filenames()
    ,   input_path("")
    ,   output_filename("")
    ,   variables_cache()
    ,   dimensions_cache()
    ,   exclude_variables(false)
    ,   join_name("")
    ,   alphabetize(true)
    ,   process_all_coords(false)
    ,   process_coords(true)
    ,   modify_history(true)
    ,   process_topology(true)
    ,   help(false)
    ,   append(false)
    ,   overwrite(false)
    ,   fix_record_dimension(false)
    ,   record_dimension_size(-1)
    ,   header_pad(-1)
    ,   slices()
    ,   boxes()
    ,   file_format(FF_UNKNOWN)
{
    TIMING("GenericCommands::GenericCommands(int,char**)");
    init();
    parse(argc, argv);
}


void GenericCommands::init()
{
    parser.push_back(&CommandLineOption::HELP);
    parser.push_back(&CommandLineOption::CDF3);
    parser.push_back(&CommandLineOption::CDF4);
    parser.push_back(&CommandLineOption::CDF5);
    parser.push_back(&CommandLineOption::FILE_FORMAT);
    parser.push_back(&CommandLineOption::APPEND);
    parser.push_back(&CommandLineOption::ALPHABETIZE);
    parser.push_back(&CommandLineOption::NO_COORDS);
    parser.push_back(&CommandLineOption::COORDS);
    parser.push_back(&CommandLineOption::DIMENSION);
    parser.push_back(&CommandLineOption::FIX_RECORD_DIMENSION);
    parser.push_back(&CommandLineOption::HEADER_PAD);
    parser.push_back(&CommandLineOption::HISTORY);
    parser.push_back(&CommandLineOption::OUTPUT);
    parser.push_back(&CommandLineOption::OVERWRITE);
    parser.push_back(&CommandLineOption::INPUT_PATH);
    parser.push_back(&CommandLineOption::VARIABLE);
    parser.push_back(&CommandLineOption::AUXILIARY);
    parser.push_back(&CommandLineOption::EXCLUDE);
    parser.push_back(&CommandLineOption::JOIN);
    parser.push_back(&CommandLineOption::UNION);
    parser.push_back(&CommandLineOption::LATLONBOX);
}


GenericCommands::~GenericCommands()
{
    TIMING("GenericCommands::~GenericCommands()");
}


void GenericCommands::parse(int argc, char **argv)
{
    vector<string> positional_arguments;

    TIMING("GenericCommands::parse(int,char**)");

    // copy command line
    cmdline = argv[0];
    for (int i=1; i<argc; ++i) {
        cmdline.append(" ");
        cmdline.append(argv[i]);
    }
    parser.parse(argc,argv);
    positional_arguments = parser.get_positional_arguments();

    if (parser.count("help")) {
        help = true;
        return; // stop parsing
    }

    if (positional_arguments.empty()) {
        throw CommandException("input and output file arguments required");
    }

    if (parser.count("output") == 0) {
        if (positional_arguments.size() == 1) {
            throw CommandException("output file argument required");
        } else if (positional_arguments.size() == 0) {
            throw CommandException("input and output file arguments required");
        }
        output_filename = positional_arguments.back();
        input_filenames.assign(positional_arguments.begin(),
                positional_arguments.end()-1);
    } else if (parser.count("output") == 1) {
        output_filename = parser.get_argument("output");
        input_filenames = positional_arguments;
    } else if (parser.count("output") > 1) {
        throw CommandException("too many output file arguments");
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
        exclude_variables = true;
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
                    variables.insert(value);
                }
            }
        }
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

    if (parser.count("union")) {
    }

    if (parser.count("alphabetize")) {
        alphabetize = false;
    }

    if (parser.count("coords")) {
        process_all_coords = true;
    }

    if (parser.count("no-coords")) {
        process_coords = false;
    }

    if (parser.count("history")) {
        modify_history = false;
    }

    if (parser.count("topology")) {
        process_topology = false;
    }

    if (parser.count("file_format")) {
        vector<string> args = parser.get_arguments("file_format");
        string val;
        if (args.empty()) {
            throw CommandException("file format requires an argument");
        } else if (args.size() > 1) {
            throw CommandException("file format given more than one argument");
        }
        val = args.front();
        if (val == "classic") {
            file_format = FF_PNETCDF_CDF1;
        } else if (val == "64bit") {
            file_format = FF_PNETCDF_CDF2;
        } else if (val == "64data") {
            file_format = FF_PNETCDF_CDF5;
        } else {
            throw CommandException("file format string not recognized");
        }
    }

    if (parser.count("3")) {
        file_format = FF_PNETCDF_CDF1;
    }

    if (parser.count("64")) {
        file_format = FF_PNETCDF_CDF2;
    }

    if (parser.count("5")) {
        file_format = FF_PNETCDF_CDF5;
    }

    if (parser.count("append")) {
        append = true;
    }

    if (parser.count("overwrite")) {
        overwrite = true;
    }

    if (parser.count("fix_rec_dmn")) {
        fix_record_dimension = true;
    }

    if (parser.count("path")) {
        input_path = parser.get_argument("path");
    }

    if (parser.count("header_pad")) {
        string arg = parser.get_argument("header_pad");
        istringstream s(arg);
        s >> header_pad;
        if (s.fail()) {
            throw CommandException("invalid header pad argument: " + arg);
        }
    }
}


/**
 * Creates and returns a new Dataset.
 *
 * Interprets the command-line parameters for union and join aggregations.
 * Determines the file format and record size and stores locally (otherwise
 * this method would be const like the rest).
 */
Dataset* GenericCommands::get_dataset()
{
    Dataset *dataset;
    Aggregation *agg;

    if (input_filenames.empty()) {
        throw CommandException("Missing input files");
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


FileWriter* GenericCommands::get_output() const
{
    FileWriter *writer = FileWriter::open(output_filename);

    writer->append(append)
        ->overwrite(overwrite)
        ->fixed_record_dimension(record_dimension_size)
        ->header_pad(header_pad)
        ->create();

    return writer;
}


static bool var_cmp(Variable *left, Variable *right)
{
    return left->get_name() < right->get_name();
}


/**
 * Modify the set of Variables of the given Dataset based on the command-line
 * parameters.
 *
 * The user could select or exclude a set of Variables, choose to alphabetize
 * them, or whether to exclude coordinate Variables.
 *
 * @param[in] dataset the Dataset
 * @return the Variables to operate over, possibly sorted by name
 */
vector<Variable*> GenericCommands::get_variables(Dataset *dataset)
{
    vector<Variable*> vars_in;
    set<string> vars_to_keep;
    vector<Variable*> vars_out;
    vector<Variable*>::const_iterator it;
    vector<Variable*>::const_iterator end;
    Grid *grid = NULL;

    if (variables_cache.find(dataset) != variables_cache.end()) {
        return variables_cache[dataset];
    }

    vars_in = dataset->get_vars();
    grid = dataset->get_grid();

    if (variables.empty()) {
        if (exclude_variables) {
            // do nothing, keep vars_to_keep empty
        } else if (process_all_coords) {
            // do nothing, keep vars_to_keep, add coords later
        } else {
            // we want all variables
            for (it=vars_in.begin(),end=vars_in.end(); it!=end; ++it) {
                vars_to_keep.insert((*it)->get_name());
            }
        }
    } else {
        if (exclude_variables) {
            // invert given variables
            for (it=vars_in.begin(),end=vars_in.end(); it!=end; ++it) {
                string name = (*it)->get_name();
                if (variables.count(name)) {
                    // skip
                } else {
                    vars_to_keep.insert(name);
                }
            }
        } else {
            // don't invert given variables
            for (it=vars_in.begin(),end=vars_in.end(); it!=end; ++it) {
                string name = (*it)->get_name();
                if (variables.count(name)) {
                    vars_to_keep.insert(name);
                }
            }
        }
    }

    // next, if we want all coordinates, add them to the set of variables
    if (grid && process_all_coords) {
        for (it=vars_in.begin(),end=vars_in.end(); it!=end; ++it) {
            Variable *var = *it;
            if (grid->is_coordinate(var)) {
                vars_to_keep.insert(var->get_name());
            }
        }
    }

    // next, if we want all topology variables, add them
    if (grid && process_topology) {
        for (it=vars_in.begin(),end=vars_in.end(); it!=end; ++it) {
            Variable *var = *it;
            if (grid->is_topology(var)) {
                vars_to_keep.insert(var->get_name());
            }
        }
    }

    // next, if we are to process coordinate variables of selected variables,
    // add those next
    if (process_coords) {
        for (it=vars_in.begin(),end=vars_in.end(); it!=end; ++it) {
            Variable *var = *it;
            Attribute *att;
            vector<Dimension*> dims;
            vector<Dimension*>::iterator dim_it;
            // only process those variables we are already keeping
            if (!vars_to_keep.count(var->get_name())) {
                continue;
            }
            // insert parts of coordinates attribute
            att = var->get_att("coordinates");
            if (att) {
                vector<string> parts = pagoda::split(att->get_string());
                vector<string>::iterator part;
                for (part=parts.begin(); part!=parts.end(); ++part) {
                    vars_to_keep.insert(*part);
                }
            }
            // look for Variables with same name as Dimensions of current var
            dims = var->get_dims();
            for (dim_it=dims.begin(); dim_it!=dims.end(); ++dim_it) {
                string name = (*dim_it)->get_name();
                if (dataset->get_var(name)) {
                    vars_to_keep.insert(name);
                }
            }
        }
    }

    // finally, create the final list of Variables
    for (it=vars_in.begin(),end=vars_in.end(); it!=end; ++it) {
        Variable *var = *it;
        string name = var->get_name();
        if (vars_to_keep.count(name)) {
            vars_out.push_back(var);
        }
    }

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


/**
 * Modify the set of Dimensions of the given Dataset based on the command-line
 * parameters.
 *
 * Only Dimensions of the selected Variables are output.
 *
 * @param[in] dataset the Dataset
 * @return the subset of Dimensions to operate over
 */
vector<Dimension*> GenericCommands::get_dimensions(Dataset *dataset)
{
    set<string> dims_to_keep;
    vector<Dimension*> dims_out;
    vector<Dimension*> dims;
    vector<Dimension*>::iterator dim_it;
    vector<Variable*> vars;
    vector<Variable*>::iterator var_it;

    if (dimensions_cache.find(dataset) != dimensions_cache.end()) {
        return dimensions_cache[dataset];
    }

    vars = get_variables(dataset);

    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        dims = (*var_it)->get_dims();
        for (dim_it=dims.begin(); dim_it!=dims.end(); ++dim_it) {
            dims_to_keep.insert((*dim_it)->get_name());
        }
    }

    dims = dataset->get_dims();
    for (dim_it=dims.begin(); dim_it!=dims.end(); ++dim_it) {
        Dimension *dim = *dim_it;
        if (dims_to_keep.count(dim->get_name())) {
            dims_out.push_back(dim);
        }
    }

    if (alphabetize) {
        sort(dims_out.begin(), dims_out.end(), dim_cmp);
    }
    
    dimensions_cache[dataset] = dims_out;

    return dims_out;
}


vector<Attribute*> GenericCommands::get_attributes(Dataset *dataset) const
{
    vector<Attribute*> atts = dataset->get_atts();

    if (modify_history) {
        Attribute *history;
        size_t pos,limit;

        for (pos=0,limit=atts.size(); pos<limit; ++pos) {
            Attribute *current_att = atts[pos];
            if (current_att->get_name() == "history") {
                history = current_att;
                break;
            }
        }

        if (history) {
            time_t time_result = time(NULL);
            string date_cmdline = ctime(&time_result);
            TypedValues<char> *history_complete;
            vector<char> history_copy;

            // replace newline added by ctime with colon space
            date_cmdline.replace(date_cmdline.size()-1, 1, ": ");
            date_cmdline += cmdline;
            history_copy.assign(date_cmdline.begin(), date_cmdline.end());
            if (history_copy.back() != '\n') {
                history_copy.push_back('\n');
            }
            history_complete = new TypedValues<char>(history_copy);
            (*history_complete) += history->get_values();
            // the new Attribute replaces the original, which is still owned
            // by its parent Dataset and will be cleaned up when the parent
            // Dataset is deleted.  However this new Attribute will not get
            // destroyed...
            atts[pos] = new GenericAttribute("history",
                    history_complete, DataType::CHAR);
        }
    }

    return atts;
}


vector<LatLonBox> GenericCommands::get_boxes() const
{
    TIMING("GenericCommands::get_box()");
    return boxes;
}


vector<DimSlice> GenericCommands::get_slices() const
{
    TIMING("GenericCommands::get_slices()");
    return slices;
}


vector<string> GenericCommands::get_input_filenames() const
{
    return input_filenames;
}


string GenericCommands::get_output_filename() const
{
    return output_filename;
}


set<string> GenericCommands::get_variables() const
{
    return variables;
}


string GenericCommands::get_join_name() const
{
    return join_name;
}


bool GenericCommands::is_excluding_variables() const
{
    return exclude_variables;
}


bool GenericCommands::is_alphabetizing() const
{
    return alphabetize;
}


bool GenericCommands::is_processing_all_coords() const
{
    return process_all_coords;
}


bool GenericCommands::is_processing_coords() const
{
    return process_coords;
}


bool GenericCommands::is_modifying_history() const
{
    return modify_history;
}


bool GenericCommands::is_helping() const
{
    return help;
}


bool GenericCommands::is_appending() const
{
    return append;
}


bool GenericCommands::is_overwriting() const
{
    return overwrite;
}


bool GenericCommands::is_fixing_record_dimension() const
{
    return fix_record_dimension;
}


int GenericCommands::get_header_pad() const
{
    return header_pad;
}


string GenericCommands::get_usage() const
{
    return parser.get_usage();
}


FileFormat GenericCommands::get_file_format() const
{
    return file_format;
}
