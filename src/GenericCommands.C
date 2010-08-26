#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include "Aggregation.H"
#include "AggregationJoinExisting.H"
#include "AggregationUnion.H"
#include "Attribute.H"
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


GenericCommands::GenericCommands()
    :   parser()
    ,   cmdline("")
    ,   input_filenames()
    ,   output_filename("")
    ,   variables()
    ,   exclude(false)
    ,   join_name("")
    ,   alphabetize(true)
    ,   all_coords(false)
    ,   process_coords(true)
    ,   modify_history(true)
    ,   process_topology(true)
    ,   help(false)
    ,   slices()
    ,   boxes()
{
    TIMING("GenericCommands::GenericCommands()");
    init();
}


GenericCommands::GenericCommands(int argc, char **argv)
    :   parser()
    ,   cmdline("")
    ,   input_filenames()
    ,   output_filename("")
    ,   variables()
    ,   exclude(false)
    ,   join_name("")
    ,   alphabetize(true)
    ,   all_coords(false)
    ,   process_coords(true)
    ,   modify_history(true)
    ,   process_topology(true)
    ,   help(false)
    ,   slices()
    ,   boxes()
{
    TIMING("GenericCommands::GenericCommands(int,char**)");
    init();
    parse(argc, argv);
}


void GenericCommands::init()
{
    parser.push_back(&CommandLineOption::HELP);
    parser.push_back(&CommandLineOption::OUTPUT);
    parser.push_back(&CommandLineOption::AUXILIARY);
    parser.push_back(&CommandLineOption::LATLONBOX);
    parser.push_back(&CommandLineOption::DIMENSION);
    parser.push_back(&CommandLineOption::VARIABLE);
    parser.push_back(&CommandLineOption::EXCLUDE);
    parser.push_back(&CommandLineOption::ALPHABETIZE);
    parser.push_back(&CommandLineOption::COORDS);
    parser.push_back(&CommandLineOption::NO_COORDS);
    parser.push_back(&CommandLineOption::JOIN);
    parser.push_back(&CommandLineOption::UNION);
    parser.push_back(&CommandLineOption::HISTORY);
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

    if (parser.count("alphabetize")) {
        alphabetize = false;
    }

    if (parser.count("coords")) {
        all_coords = true;
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
}


Dataset* GenericCommands::get_dataset() const
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


FileWriter* GenericCommands::get_output() const
{
    return FileWriter::create(output_filename);
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
vector<Variable*> GenericCommands::get_variables(Dataset *dataset) const
{
    const vector<Variable*> vars_in = dataset->get_vars();
    set<string> vars_to_keep;
    vector<Variable*> vars_out;
    vector<Variable*>::const_iterator it;
    vector<Variable*>::const_iterator end;
    Grid *grid = dataset->get_grid();

    if (variables.empty()) {
        if (exclude) {
            // do nothing, keep vars_to_keep empty
        } else if (all_coords) {
            // do nothing, keep vars_to_keep, add coords later
        } else {
            // we want all variables
            for (it=vars_in.begin(),end=vars_in.end(); it!=end; ++it) {
                vars_to_keep.insert((*it)->get_name());
            }
        }
    } else {
        if (exclude) {
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
    if (grid && all_coords) {
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
vector<Dimension*> GenericCommands::get_dimensions(Dataset *dataset) const
{
    set<string> dims_to_keep;
    vector<Dimension*> dims_out;
    vector<Dimension*> dims;
    vector<Dimension*>::iterator dim_it;
    vector<Variable*> vars = get_variables(dataset);
    vector<Variable*>::iterator var_it;

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


bool GenericCommands::get_exclude() const
{
    return exclude;
}


string GenericCommands::get_join_name() const
{
    return join_name;
}

bool GenericCommands::get_alphabetize() const
{
    return alphabetize;
}


bool GenericCommands::get_all_coords() const
{
    return all_coords;
}

bool GenericCommands::get_process_coords() const
{
    return process_coords;
}


bool GenericCommands::get_modify_history() const
{
    return modify_history;
}


bool GenericCommands::get_help() const
{
    return help;
}


string GenericCommands::get_usage() const
{
    return parser.get_usage();
}