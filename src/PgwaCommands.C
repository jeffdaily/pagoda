#ifdef HAVE_CONFIG_H
#   include <config.h>
#endif

#include <algorithm>
#include <sstream>

#include "Aggregation.H"
#include "AggregationJoinExisting.H"
#include "CommandException.H"
#include "CommandLineOption.H"
#include "Dataset.H"
#include "Dimension.H"
#include "Error.H"
#include "FileWriter.H"
#include "GenericCommands.H"
#include "PgwaCommands.H"
#include "Util.H"
#include "ValidMaskCondition.H"
#include "Variable.H"

using std::find;
using std::ostringstream;


vector<string> PgwaCommands::VALID;


PgwaCommands::PgwaCommands()
    :   GenericCommands()
    ,   op_type("")
    ,   mask_value("")
    ,   mask_variable("")
    ,   mask_comparator("")
    ,   weight_variable("")
    ,   dimensions()
    ,   retain_degenerate_dimensions(false)
{
    init();
}


PgwaCommands::PgwaCommands(int argc, char **argv)
    :   GenericCommands()
    ,   op_type("")
    ,   mask_value("")
    ,   mask_variable("")
    ,   mask_comparator("")
    ,   weight_variable("")
{
    init();
    parse(argc, argv);
}


PgwaCommands::~PgwaCommands()
{
}


void PgwaCommands::parse(int argc, char **argv)
{
    GenericCommands::parse(argc,argv);

    if (parser.count(CommandLineOption::AVERAGE_OPERATION)) {
        op_type = parser.get_argument(CommandLineOption::AVERAGE_OPERATION);
        if (find(VALID.begin(),VALID.end(),op_type) == VALID.end()) {
            throw CommandException("operator '" + op_type + "' not recognized");
        }
    }
    else {
        op_type = OP_AVG;
    }

    if (parser.count(CommandLineOption::MASK_CONDITION)) {
        // mask conditions are of the form "ORO < 1"
        // mask conditions are of the form "ORO<1"
        string arg = parser.get_argument(CommandLineOption::MASK_CONDITION);
        vector<string> ops;
        string op;
        string front;
        string back;
        size_t pos;
        double value;
        istringstream value_string;

        ops.push_back(OP_EQ_SYM);
        ops.push_back(OP_NE_SYM);
        ops.push_back(OP_GE_SYM);
        ops.push_back(OP_LE_SYM);
        ops.push_back(OP_GT_SYM);
        ops.push_back(OP_LT_SYM);
        for (vector<string>::iterator it=ops.begin(); it!=ops.end(); ++it) {
            op = *it;
            if (string::npos != (pos = arg.find(op))) {
                break;
            }
        }
        if (string::npos == pos) {
            ostringstream err;
            err << "Mask string ("
                << arg
                << ") does not contain valid comparison operator";
            throw CommandException(err.str());
        }
        front = arg.substr(0, pos);
        back = arg.substr(pos+op.size());
        pagoda::trim(front);
        pagoda::trim(back);
        /* which of front or back is the variable and which is the number? */
        value_string.str(front);
        value_string >> value;
        if (value_string) {
            mask_value = front;
            mask_variable = back;
            mask_comparator = op;
        }
        else {
            /* try the back as the number */
            value_string.clear();
            value_string.str(back);
            value_string >> value;
            if (value_string) {
                mask_value = back;
                mask_variable = front;
                mask_comparator = op;
            }
            else {
                ostringstream err;
                err << "Mask string ("
                    << arg
                    << ") does not contain valid number";
                throw CommandException(err.str());
            }
        }
        /* make sure the assumed mask variable isn't also a number */
        value_string.clear();
        value_string.str(mask_variable);
        value_string >> value;
        if (value_string) {
            ostringstream err;
            err << "Mask string ("
                << arg
                << ") does not contain valid variable name";
            throw CommandException(err.str());
        }
    }
    else {
        mask_value = "1.0";
        if (parser.count(CommandLineOption::MASK_COMPARATOR)) {
            mask_comparator = parser.get_argument(CommandLineOption::MASK_COMPARATOR);
        }
        if (parser.count(CommandLineOption::MASK_NAME)) {
            mask_variable = parser.get_argument(CommandLineOption::MASK_NAME);
        }
        if (parser.count(CommandLineOption::MASK_VALUE)) {
            mask_value = parser.get_argument(CommandLineOption::MASK_VALUE);
        }
    }

    if (parser.count(CommandLineOption::RETAIN_DEGENERATE_DIMENSIONS)) {
        retain_degenerate_dimensions = true;
    }

    if (parser.count(CommandLineOption::AVERAGE_DIMENSIONS)) {
        vector<string> args =
            parser.get_arguments(CommandLineOption::AVERAGE_DIMENSIONS);
        for (vector<string>::iterator it=args.begin(); it!=args.end(); ++it) {
            istringstream iss(*it);
            while (iss) {
                string value;
                getline(iss, value, ',');
                if (!value.empty()) {
                    dimensions.insert(value);
                }
            }
        }
    }

    if (parser.count(CommandLineOption::WEIGHTING_VARIABLE_NAME)) {
        weight_variable = parser.get_argument(
                CommandLineOption::WEIGHTING_VARIABLE_NAME);
    }
}


void PgwaCommands::get_inputs_and_outputs(Dataset *&dataset,
        vector<Variable*> &vars, FileWriter* &writer)
{
    vector<Attribute*> atts;
    vector<Dimension*> dims;

    get_inputs(dataset, vars, dims, atts);
    writer = get_output();
    writer->write_atts(atts);
    for (vector<Dimension*>::iterator dim_it=dims.begin();
            dim_it!=dims.end(); ++dim_it) {
        Dimension *dim = *dim_it;
        if (dimensions.empty() || dimensions.count(dim->get_name()) != 0) {
            if (retain_degenerate_dimensions) {
                writer->def_dim(dim->get_name(), 1);
            }
        } else {
            writer->def_dim(dim);
        }
    }
    if (retain_degenerate_dimensions) {
        // a var's dims are defined by name, so the dim sizes here don't matter
        writer->def_vars(vars);
    } else {
        // we must define the vars without the dimensions getting removed
        for (vector<Variable*>::iterator var_it=vars.begin();
                var_it!=vars.end(); ++var_it) {
            Variable *var = *var_it;
            vector<Dimension*> dims = var->get_dims();
            vector<string> dim_names;
            for (vector<Dimension*>::iterator dim_it=dims.begin();
                    dim_it!=dims.end(); ++dim_it) {
                Dimension *dim = *dim_it;
                if (dimensions.empty()
                        || dimensions.count(dim->get_name()) != 0) {
                    /* this dim is eliminated */
                }
                else {
                    dim_names.push_back(dim->get_name());
                }
            }
            writer->def_var(var->get_name(), dim_names, var->get_type(),
                    var->get_atts());
        }
    }
}


string PgwaCommands::get_operator() const
{
    return op_type;
}


Variable* PgwaCommands::get_mask_variable()
{
    Dataset *dataset = NULL;
    Variable *var = NULL;
    DataType type(DataType::NOT_A_TYPE);
    Validator *validator = NULL;

    /* return immediately if no mask variable was specified */
    if (mask_variable.empty()) {
        return NULL;
    }

    dataset = get_dataset();
    var = dataset->get_var(mask_variable);

    if (NULL == var) {
        ostringstream err;
        err << "requested variable '"
            << mask_variable
            << "' is not in input file";
        throw CommandException(err.str());
    }

    type = var->get_type();
#define DATATYPE_EXPAND(DT,T) \
    if (type == DT) { \
        validator = new ValidMaskCondition<T>(mask_comparator, mask_value); \
    } else
#include "DataType.def"
    {
        ERR("DataType not handled");
    }

    var->set_validator(validator);

    return var;
}


Variable* PgwaCommands::get_weight_variable()
{
    Dataset *dataset = NULL;
    Variable *var = NULL;

    /* return immediately if no weight variable was specified */
    if (weight_variable.empty()) {
        return NULL;
    }

    dataset = get_dataset();
    var = dataset->get_var(weight_variable);

    if (NULL == var) {
        ostringstream err;
        err << "requested variable '"
            << weight_variable
            << "' is not in input file";
        throw CommandException(err.str());
    }

    return var;
}


set<string> PgwaCommands::get_reduced_dimensions() const
{
    return dimensions;
}


void PgwaCommands::init()
{
    // alphabetize 'a' is replaced with average dimensions 'a'
    //parser.erase(CommandLineOption::ALPHABETIZE);
    parser.push_back(CommandLineOption::AVERAGE_OPERATION);
    parser.push_back(CommandLineOption::AVERAGE_DIMENSIONS);
    parser.push_back(CommandLineOption::MASK_CONDITION);
    parser.push_back(CommandLineOption::MASK_COMPARATOR);
    parser.push_back(CommandLineOption::MASK_NAME);
    parser.push_back(CommandLineOption::MASK_VALUE);
    parser.push_back(CommandLineOption::WEIGHT_MASK_COORDINATE_VARIABLES);
    parser.push_back(CommandLineOption::WEIGHTING_VARIABLE_NAME);
    parser.push_back(CommandLineOption::NO_NORMALIZATION);
    parser.push_back(CommandLineOption::RETAIN_DEGENERATE_DIMENSIONS);
    // erase the aggregation ops
    //parser.erase(CommandLineOption::JOIN);
    //parser.erase(CommandLineOption::UNION);

    if (VALID.empty()) {
        VALID.push_back(OP_AVG);
        VALID.push_back(OP_SQRAVG);
        VALID.push_back(OP_AVGSQR);
        VALID.push_back(OP_MAX);
        VALID.push_back(OP_MIN);
        VALID.push_back(OP_RMS);
        VALID.push_back(OP_RMSSDN);
        VALID.push_back(OP_SQRT);
        VALID.push_back(OP_TTL);
    }
}
