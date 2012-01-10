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
#include "PgboCommands.H"
#include "Print.H"

using std::find;


vector<string> PgboCommands::ADD;
vector<string> PgboCommands::SUB;
vector<string> PgboCommands::MUL;
vector<string> PgboCommands::DIV;



PgboCommands::PgboCommands()
    :   GenericCommands()
    ,   op_type("")
    ,   operand_filename()
{
    init();
}


PgboCommands::PgboCommands(int argc, char **argv)
    :   GenericCommands()
    ,   op_type("")
    ,   operand_filename()
{
    init();
    parse(argc, argv);
}


PgboCommands::~PgboCommands()
{
}


void PgboCommands::parse(int argc, char **argv)
{
    GenericCommands::parse(argc,argv);

    if (input_filenames.size() != 2) {
        throw CommandException("two and only input files required");
    }
    operand_filename = input_filenames[1];
    input_filenames.resize(1); // pop

    if (parser.count(CommandLineOption::BINARY_OPERATION)) {
        vector<string> valid;
        valid.insert(valid.end(), ADD.begin(), ADD.end());
        valid.insert(valid.end(), SUB.begin(), SUB.end());
        valid.insert(valid.end(), MUL.begin(), MUL.end());
        valid.insert(valid.end(), DIV.begin(), DIV.end());
        op_type = parser.get_argument(CommandLineOption::BINARY_OPERATION);
        if (find(valid.begin(),valid.end(),op_type) == valid.end()) {
            throw CommandException("operator '" + op_type + "' not recognized");
        }
    }
    else {
        throw CommandException("operator is required");
    }
}


void PgboCommands::get_inputs_and_outputs(Dataset* &dataset,
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


Dataset* PgboCommands::get_operand() const
{
    return Dataset::open(operand_filename);
}


string PgboCommands::get_operator() const
{
    // normalize the possible results
    if (find(ADD.begin(),ADD.end(),op_type) != ADD.end()) {
        return OP_ADD;
    }
    else if (find(SUB.begin(),SUB.end(),op_type) != SUB.end()) {
        return OP_SUB;
    }
    else if (find(MUL.begin(),MUL.end(),op_type) != MUL.end()) {
        return OP_MUL;
    }
    else if (find(DIV.begin(),DIV.end(),op_type) != DIV.end()) {
        return OP_DIV;
    }
    else {
        throw CommandException("could not normalize operator");
    }
}


void PgboCommands::init()
{
    parser.push_back(CommandLineOption::BINARY_OPERATION);
    // erase the aggregation ops
    parser.erase(CommandLineOption::JOIN);
    parser.erase(CommandLineOption::UNION);

    if (ADD.empty()) {
        ADD.push_back("add");
        ADD.push_back("+");
        ADD.push_back("addition");
    }
    if (SUB.empty()) {
        SUB.push_back("sbt");
        SUB.push_back("-");
        SUB.push_back("dff");
        SUB.push_back("diff");
        SUB.push_back("sub");
        SUB.push_back("subtract");
        SUB.push_back("subtraction");
    }
    if (MUL.empty()) {
        MUL.push_back("mlt");
        MUL.push_back("*");
        MUL.push_back("mult");
        MUL.push_back("multiply");
        MUL.push_back("multiplication");
    }
    if (DIV.empty()) {
        DIV.push_back("dvd");
        DIV.push_back("/");
        DIV.push_back("divide");
        DIV.push_back("division");
    }
}
