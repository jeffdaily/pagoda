#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <ctime>
#include <vector>

#include "Aggregation.H"
#include "AggregationJoinExisting.H"
#include "AggregationUnion.H"
#include "Attribute.H"
#include "Bootstrap.H"
#include "CommandException.H"
#include "CommandLineOption.H"
#include "CommandLineParser.H"
#include "Common.H"
#include "CoordHyperslab.H"
#include "Dataset.H"
#include "Dimension.H"
#include "Error.H"
#include "FileWriter.H"
#include "GenericAttribute.H"
#include "GenericCommands.H"
#include "Grid.H"
#include "Hints.H"
#include "MaskMap.H"
#include "PagodaException.H"
#include "Print.H"
#include "TypedValues.H"
#include "Util.H"
#include "Variable.H"


using std::sort;
using std::vector;


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
    ,   append(false)
    ,   overwrite(false)
    ,   fix_record_dimension(false)
    ,   record_dimension_size(-1)
    ,   header_pad(-1)
    ,   index_hyperslabs()
    ,   boxes()
    ,   file_format(FF_UNKNOWN)
    ,   nonblocking_io(false)
    ,   reading_all_records(false)
    ,   reading_all_variables(false)
    ,   histories()
{
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
    ,   append(false)
    ,   overwrite(false)
    ,   fix_record_dimension(false)
    ,   record_dimension_size(-1)
    ,   header_pad(-1)
    ,   index_hyperslabs()
    ,   boxes()
    ,   file_format(FF_UNKNOWN)
    ,   nonblocking_io(false)
    ,   reading_all_records(false)
    ,   reading_all_variables(false)
    ,   histories()
{
    init();
    parse(argc, argv);
}


void GenericCommands::init()
{
    parser.push_back(&CommandLineOption::HELP);
    parser.push_back(&CommandLineOption::VERS);
    parser.push_back(&CommandLineOption::CDF1);
    parser.push_back(&CommandLineOption::CDF2);
    parser.push_back(&CommandLineOption::CDF5);
    parser.push_back(&CommandLineOption::NC4);
    parser.push_back(&CommandLineOption::NC4_CLASSIC);
    parser.push_back(&CommandLineOption::FILE_FORMAT);
    parser.push_back(&CommandLineOption::NONBLOCKING_IO);
    parser.push_back(&CommandLineOption::CB_BUFFER_SIZE);
    parser.push_back(&CommandLineOption::ROMIO_CB_READ);
    parser.push_back(&CommandLineOption::ROMIO_DS_READ);
    parser.push_back(&CommandLineOption::STRIPING_UNIT);
    parser.push_back(&CommandLineOption::READ_ALL_RECORDS);
    parser.push_back(&CommandLineOption::READ_ALL_VARIABLES);
    parser.push_back(&CommandLineOption::APPEND);
    parser.push_back(&CommandLineOption::ALPHABETIZE);
    parser.push_back(&CommandLineOption::NO_COORDS);
    parser.push_back(&CommandLineOption::COORDS);
    parser.push_back(&CommandLineOption::TOPOLOGY);
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
    for (size_t i=0,limit=histories.size(); i<limit; ++i) {
        delete histories[i];
    }
}


void GenericCommands::parse(int argc, char **argv)
{
    vector<string> positional_arguments;


    // copy command line
    cmdline = argv[0];
    for (int i=1; i<argc; ++i) {
        cmdline.append(" ");
        cmdline.append(argv[i]);
    }
    parser.parse(argc,argv);
    positional_arguments = parser.get_positional_arguments();

    if (parser.count("help")) {
        pagoda::print_zero(get_usage());
        pagoda::finalize();
        exit(EXIT_SUCCESS);
    }

    if (parser.count("version")) {
        pagoda::print_zero(get_version());
        pagoda::finalize();
        exit(EXIT_SUCCESS);
    }

    if (positional_arguments.empty()) {
        if (parser.count("output")) {
            throw CommandException("input file(s) required");
        } else {
            throw CommandException("input and output file arguments required");
        }
    }
    else if (1 == positional_arguments.size() && 0 == parser.count("output")) {
        throw CommandException("output file argument required");
    }
    else {
        input_filenames = positional_arguments;
        if (parser.count("output") == 0) {
            input_filenames.resize(input_filenames.size()-1); // pop
        }
    }

    if (parser.count("output")) {
        output_filename = parser.get_argument("output");
    } else {
        output_filename = positional_arguments.back();
    }

    if (parser.count("path")) {
        input_path = parser.get_argument("path");
        if (input_path.empty()) {
            throw CommandException("empty input path");
        }
        if (!pagoda::ends_with(input_path, "/")) {
            input_path += "/";
        }
        // prepend the input path
        for (size_t i=0,limit=input_filenames.size(); i<limit; ++i)  {
            input_filenames[i] = input_path + input_filenames[i];
        }
    }

    // sanity check -- also for a more readble error message
    // this might not be efficient since it internally involves a broadcast
    // but it's better to know this early instead of the verbose error message
    // produced by the Dataset::open() later.
    for (size_t i=0,limit=input_filenames.size(); i<limit; ++i)  {
        if (!pagoda::file_exists(input_filenames[i])) {
            throw CommandException("file does not exist: "+input_filenames[i]);
        }
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
            if (it->find(".") == string::npos) {
                index_hyperslabs.push_back(IndexHyperslab(*it));
            } else {
                coord_hyperslabs.push_back(CoordHyperslab(*it));
            }
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
        }
        else if (args.size() > 1) {
            throw CommandException("file format given more than one argument");
        }
        val = args.front();
        if (val == "classic") {
            file_format = FF_CDF1;
        }
        else if (val == "64bit") {
            file_format = FF_CDF2;
        }
        else if (val == "64data") {
            file_format = FF_CDF5;
        }
        else if (val == "netcdf4") {
            file_format = FF_NETCDF4;
        }
        else if (val == "netcdf4_classic") {
            file_format = FF_NETCDF4_CLASSIC;
        }
        else {
            throw CommandException("file format string not recognized");
        }
    }

    if (parser.count("3")) {
        file_format = FF_CDF1;
    }

    if (parser.count("64")) {
        file_format = FF_CDF2;
    }

    if (parser.count("5")) {
        file_format = FF_CDF5;
    }

    if (parser.count("nbio")) {
        nonblocking_io = true;
    }

    if (parser.count("allrec")) {
        reading_all_records = true;
    }

    if (parser.count("allvar")) {
        reading_all_variables = true;
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

    if (parser.count("header_pad")) {
        string arg = parser.get_argument("header_pad");
        istringstream s(arg);
        s >> header_pad;
        if (s.fail()) {
            throw CommandException("invalid header pad argument: " + arg);
        }
    }

    if (parser.count("cb_buffer_size")) {
        Hints::cb_buffer_size = parser.get_argument("cb_buffer_size");
    }
    if (parser.count("romio_cb_read")) {
        Hints::romio_cb_read = parser.get_argument("romio_cb_read");
    }
    if (parser.count("romio_ds_read")) {
        Hints::romio_ds_read = parser.get_argument("romio_ds_read");
    }
    if (parser.count("striping_unit")) {
        Hints::striping_unit = parser.get_argument("striping_unit");
    }
    pagoda::println_zero("Hints\n" + Hints::to_string());
}


/* often we don't need the dims and atts returned... */
void GenericCommands::get_inputs_and_outputs(Dataset *&dataset,
        vector<Variable*> &vars, FileWriter* &writer)
{
    vector<Attribute*> atts;
    vector<Dimension*> dims;

    get_inputs(dataset, vars, dims, atts);
    writer = get_output();
    writer->write_atts(atts);
    writer->def_dims(dims);
    writer->def_vars(vars);
}


void GenericCommands::get_inputs(Dataset *&dataset, vector<Variable*> &vars,
        vector<Dimension*> &dims, vector<Attribute*> &atts)
{
    Grid *grid = NULL;
    vector<Grid*> grids;
    MaskMap *masks = NULL;

    dataset = get_dataset();
    vars = get_variables(dataset);
    dims = get_dimensions(dataset);
    atts = get_attributes(dataset);
    grids = dataset->get_grids();
    if (grids.empty()) {
        pagoda::println_zero("no grid found");
    } else {
        grid = grids[0];
    }

    masks = new MaskMap(dataset);
    masks->modify(get_index_hyperslabs());
    masks->modify(get_coord_hyperslabs(), grid);
    masks->modify(get_boxes(), grid);
    dataset->set_masks(masks);
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


FileWriter* GenericCommands::get_output() const
{
    FileWriter *writer = FileWriter::open(output_filename, file_format);

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
        }
        else if (process_all_coords) {
            // do nothing, keep vars_to_keep, add coords later
        }
        else {
            // we want all variables
            for (it=vars_in.begin(),end=vars_in.end(); it!=end; ++it) {
                vars_to_keep.insert((*it)->get_name());
            }
        }
    }
    else {
        if (exclude_variables) {
            // invert given variables
            for (it=vars_in.begin(),end=vars_in.end(); it!=end; ++it) {
                string name = (*it)->get_name();
                if (variables.count(name)) {
                    // skip
                }
                else {
                    vars_to_keep.insert(name);
                }
            }
        }
        else {
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


vector<Attribute*> GenericCommands::get_attributes(Dataset *dataset)
{
    vector<Attribute*> atts = dataset->get_atts();

    if (modify_history) {
        Attribute *history = NULL;
        size_t pos;
        size_t limit;
        time_t time_result;
        string date_cmdline;
        TypedValues<char> *history_complete;
        vector<char> history_copy;

        if (pagoda::me == 0) {
            time_result = time(NULL);
        } else {
            time_result = 0;
        }
        pagoda::broadcast(time_result, 0);
        date_cmdline = ctime(&time_result);

        // replace newline added by ctime with colon space
        date_cmdline.replace(date_cmdline.size()-1, 1, ": ");
        date_cmdline += cmdline;
        history_copy.assign(date_cmdline.begin(), date_cmdline.end());

        for (pos=0,limit=atts.size(); pos<limit; ++pos) {
            Attribute *current_att = atts[pos];
            if (current_att->get_name() == "history") {
                history = current_att;
                break;
            }
        }

        if (history) {
            if (history_copy.back() != '\n') {
                history_copy.push_back('\n');
            }
            history_complete = new TypedValues<char>(history_copy);
            (*history_complete) += history->get_values();
            // the new Attribute replaces the original, which is still owned
            // by its parent Dataset and will be cleaned up when the parent
            // Dataset is deleted.  However this new Attribute will not get
            // destroyed which is a memory leak.
            /** @todo cache Attribute in this GenericCommands instance */
            histories.push_back(new GenericAttribute("history",
                        history_complete, DataType::CHAR));
            // this is safe because Dataset instances delete their own atts,
            // not those in this atts vector
            atts[pos] = histories.back();
        }
        else {
            history_complete = new TypedValues<char>(history_copy);
            histories.push_back(new GenericAttribute("history",
                        history_complete, DataType::CHAR));
            atts.push_back(histories.back());
        }
    }

    return atts;
}


vector<LatLonBox> GenericCommands::get_boxes() const
{
    return boxes;
}


vector<IndexHyperslab> GenericCommands::get_index_hyperslabs() const
{
    return index_hyperslabs;
}


vector<CoordHyperslab> GenericCommands::get_coord_hyperslabs() const
{
    return coord_hyperslabs;
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


string GenericCommands::get_version() const
{
    return PACKAGE_STRING;
}


FileFormat GenericCommands::get_file_format() const
{
    return file_format;
}


string GenericCommands::get_input_path() const
{
    return input_path;
}


bool GenericCommands::is_nonblocking() const
{
    return nonblocking_io;
}


bool GenericCommands::is_reading_all_records() const
{
    return reading_all_records;
}


bool GenericCommands::is_reading_all_variables() const
{
    return reading_all_variables;
}
