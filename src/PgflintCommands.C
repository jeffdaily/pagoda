#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>

#include "CommandException.H"
#include "CommandLineOption.H"
#include "Dataset.H"
#include "Error.H"
#include "FileWriter.H"
#include "GenericCommands.H"
#include "MaskMap.H"
#include "PgflintCommands.H"
#include "Print.H"

using std::find;


PgflintCommands::PgflintCommands()
    :   GenericCommands()
    ,   operand_filename()
#if ENABLE_INTERPOLANT
    ,   interpolant("")
    ,   value(0.0)
#endif
    ,   weight1(0.5)
    ,   weight2(0.5)
{
    init();
}


PgflintCommands::PgflintCommands(int argc, char **argv)
    :   GenericCommands()
    ,   operand_filename()
#if ENABLE_INTERPOLANT
    ,   interpolant("")
    ,   value(0.0)
#endif
    ,   weight1(0.5)
    ,   weight2(0.5)
{
    init();
    parse(argc, argv);
}


PgflintCommands::~PgflintCommands()
{
}


void PgflintCommands::parse(int argc, char **argv)
{
    GenericCommands::parse(argc,argv);

    if (input_filenames.size() != 2) {
        throw CommandException("two and only input files required");
    }
    operand_filename = input_filenames[1];
    input_filenames.resize(1); // pop

#if ENABLE_INTERPOLANT
    if (parser.count(CommandLineOption::WEIGHTING_VARIABLE_NAME)
            && parser.count(CommandLineOption::INTERPOLATE)) {
        throw CommandException("cannot specify both weight and interpolate");
    }
    else if (parser.count(CommandLineOption::WEIGHTING_VARIABLE_NAME)) {
#endif
    if (parser.count(CommandLineOption::WEIGHTING_VARIABLE_NAME)) {
        string weight = parser.get_argument(CommandLineOption::WEIGHTING_VARIABLE_NAME);
        vector<string> parts = pagoda::split(weight,',');
        if (parts.size() != 1 && parts.size() != 2) {
            throw CommandException("invalid weight string");
        }
        else {
            istringstream weight1_string(parts[0]);
            weight1_string >> weight1;
            if (weight1_string.fail()) {
                throw CommandException("invalid weight string");
            }
            if (parts.size() == 2) {
                istringstream weight2_string(parts[1]);
                weight2_string >> weight2;
                if (weight2_string.fail()) {
                    throw CommandException("invalid weight string");
                }
            } else {
                weight2 = 1.0 - weight1;
            }
        }
    }
#if ENABLE_INTERPOLANT
    else if (parser.count(CommandLineOption::INTERPOLATE)) {
        string interpolant_string = parser.get_argument(CommandLineOption::INTERPOLATE);
        vector<string> parts = pagoda::split(interpolant_string,',');
        if (parts.size() != 2) {
            throw CommandException("invalid interpolate string");
        }
        else {
            interpolant = parts[0];
            istringstream value_string(parts[1]);
            value_string >> value;
            if (value_string.fail()) {
                throw CommandException("invalid interpolate string");
            }
        }
    }
#endif
}


void PgflintCommands::get_inputs_and_outputs(Dataset* &dataset,
                                          Dataset* &operand,
                                          vector<Variable*> &vars,
                                          FileWriter* &writer)
{
    vector<Attribute*> atts;
    vector<Dimension*> dims;
    vector<Grid*> grids;
    Grid *ds_grid = NULL;
    Grid *op_grid = NULL;
    MaskMap *ds_masks = NULL;
    MaskMap *op_masks = NULL;

    dataset = get_dataset();
    operand = get_operand();
    vars = get_variables(dataset);
    dims = get_dimensions(dataset);
    atts = get_attributes(dataset);

    grids = dataset->get_grids();
    if (grids.empty()) {
        pagoda::println_zero("no grid found in dataset");
    } else {
        ds_grid = grids[0];
    }

    grids = operand->get_grids();
    if (grids.empty()) {
        pagoda::println_zero("no grid found in operand");
    } else {
        op_grid = grids[0];
    }

    ds_masks = new MaskMap(dataset);
    ds_masks->modify(get_index_hyperslabs());
    ds_masks->modify(get_coord_hyperslabs(), ds_grid);
    ds_masks->modify(get_boxes(), ds_grid);
    dataset->set_masks(ds_masks);

    op_masks = new MaskMap(operand);
    op_masks->modify(get_index_hyperslabs());
    op_masks->modify(get_coord_hyperslabs(), op_grid);
    op_masks->modify(get_boxes(), op_grid);
    operand->set_masks(op_masks);

    writer = get_output();
    writer->write_atts(atts);
    writer->def_dims(dims);
    writer->def_vars(vars);
}


Dataset* PgflintCommands::get_operand() const
{
    return Dataset::open(operand_filename);
}


#if ENABLE_INTERPOLANT
bool PgflintCommands::has_interpolant() const
{
    return !interpolant.empty();
}


string PgflintCommands::get_interpolant() const
{
    return interpolant;
}


double PgflintCommands::get_value() const
{
    return value;
}
#endif


double PgflintCommands::get_weight1() const
{
    return weight1;
}


double PgflintCommands::get_weight2() const
{
    return weight2;
}


void PgflintCommands::init()
{
#if ENABLE_INTERPOLANT
    parser.push_back(CommandLineOption::INTERPOLATE);
#endif
    parser.push_back(CommandLineOption::WEIGHTS_OF_FILES);
}
