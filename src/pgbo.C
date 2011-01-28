#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cstdlib>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "Array.H"
#include "Bootstrap.H"
#include "CommandException.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "Error.H"
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "PgboCommands.H"
#include "Util.H"
#include "Variable.H"

using std::exception;
using std::string;
using std::map;
using std::vector;


#ifdef F77_DUMMY_MAIN
#   ifdef __cplusplus
extern "C"
#   endif
int F77_DUMMY_MAIN()
{
    return 1;
}
#endif

//#define READ_ALL
//#define READ_RECORD
//#define READ_NONBLOCKING


static void write(Variable *var, FileWriter *writer);
static void write_shape_match_per_record(
    Variable *lhs, Variable *rhs, string op, FileWriter *writer);
static void write_shape_match(
    Variable *lhs, Variable *rhs, string op, FileWriter *writer);
static void do_op(Array *lhs, string op, Array *rhs);


int main(int argc, char **argv)
{
    PgboCommands cmd;
    string op_type;
    Dataset *dataset;
    vector<Variable*> ds_vars;
    vector<Dimension*> ds_dims;
    MaskMap *ds_masks;
    Dataset *operand;
    vector<Variable*> op_vars;
    vector<Dimension*> op_dims;
    MaskMap *op_masks;
    vector<Variable*>::iterator var_it;
    FileWriter *writer;
    vector<Grid*> grids;
    Grid *grid;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);

        if (cmd.is_requesting_info()) {
            pagoda::print_zero(cmd.get_info());
            pagoda::finalize();
            return EXIT_SUCCESS;
        }

        dataset = cmd.get_dataset();
        operand = cmd.get_operand();
        op_type = cmd.get_operator();

        ds_vars = cmd.get_variables(dataset);
        op_vars = cmd.get_variables(operand);
        ds_dims = cmd.get_dimensions(dataset);
        op_dims = cmd.get_dimensions(operand);

        // okay, we assume dataset and operand have the same grid
        grids = dataset->get_grids();
        if (grids.empty()) {
            pagoda::print_zero("no grid found\n");
            grid = NULL;
        }
        else {
            grid = grids[0];
        }

        // create masks for dataset and operand
        ds_masks = new MaskMap(dataset);
        ds_masks->modify(cmd.get_index_hyperslabs());
        ds_masks->modify(cmd.get_coord_hyperslabs(), grid);
        ds_masks->modify(cmd.get_boxes(), grid);
        op_masks = new MaskMap(operand);
        op_masks->modify(cmd.get_index_hyperslabs());
        op_masks->modify(cmd.get_coord_hyperslabs(), grid);
        op_masks->modify(cmd.get_boxes(), grid);

        // create the output writer
        writer = cmd.get_output();
        writer->write_atts(cmd.get_attributes(dataset));
        writer->def_dims(ds_dims);
        writer->def_vars(ds_vars);

        // handle one variable at a time
        for (var_it=ds_vars.begin(); var_it!=ds_vars.end(); ++var_it) {
            Variable *ds_var = *var_it;
            Variable *op_var = operand->get_var(ds_var->get_name());
            if (!op_var) {
                // no corresponding variable in the operand dataset,
                // so copy ds_var unchanged to output
                pagoda::print_zero(
                    "missing operand variable " + ds_var->get_name());
                write(ds_var, writer);
            }
            else if (grid->is_coordinate(ds_var)
                     || grid->is_topology(ds_var)) {
                // we have a coordinate or topology variable,
                // so copy ds_var unchanged to output
                write(ds_var, writer);
            }
            else if (ds_var->get_ndim() < op_var->get_ndim()) {
                ERR("operand variable rank < input variable rank");
            }
            else if (ds_var->get_shape() == op_var->get_shape()) {
                // shapes of each var match
                // if either variable is record-based, both are treated as such
                if (ds_var->has_record() || op_var->has_record()) {
                    write_shape_match_per_record(
                        ds_var, op_var, op_type, writer);
                }
                else {
                    // oh well, we tried to do something record-based
                    write_shape_match(ds_var, op_var, op_type, writer);
                }
            }
            else {
                // at this point we know the shapes are mismatched, but at
                // least they are broadcast-able
            }
        }

        // clean up
        delete dataset;
        delete operand;
        delete ds_masks;
        delete op_masks;
        delete writer;

        pagoda::finalize();

    }
    catch (CommandException &ex) {
        pagoda::println_zero(ex.what());
        pagoda::finalize();
        return EXIT_FAILURE;
    }
    catch (exception &ex) {
        pagoda::abort(ex.what());
    }

    return EXIT_SUCCESS;
}


/**
 * Write entire Variable to FileWriter.
 */
static void write(Variable *var, FileWriter *writer)
{
    if (var->has_record()) {
        Array *array = NULL; // reuse allocated array each record
        for (int64_t rec=0,limit=var->get_nrec(); rec<limit; ++rec) {
            array = var->read(rec, array);
            writer->write(array, var->get_name(), rec);
        }
        delete array;
    }
    else {
        Array *array = var->read();
        writer->write(array, var->get_name());
        delete array;
    }
}


static void write_shape_match_per_record(
    Variable *lhs, Variable *rhs, string op, FileWriter *writer)
{
    // operate one record at a time for both vars
    Array *lhs_array = NULL; // reuse allocated array each record
    Array *rhs_array = NULL; // reuse allocated array each record
    for (int64_t rec=0,limit=lhs->get_nrec(); rec<limit; ++rec) {
        lhs_array = lhs->read(rec, lhs_array);
        rhs_array = rhs->read(rec, rhs_array);
        do_op(lhs_array, op, rhs_array);
        writer->write(lhs_array, lhs->get_name(), rec);
    }
    delete lhs_array;
    delete rhs_array;
}


static void write_shape_match(
    Variable *lhs, Variable *rhs, string op, FileWriter *writer)
{
    Array *lhs_array = lhs->read();
    Array *rhs_array = rhs->read();
    do_op(lhs_array, op, rhs_array);
    writer->write(lhs_array, lhs->get_name());
    delete lhs_array;
    delete rhs_array;
}


static void do_op(Array *lhs, string op, Array *rhs)
{
    if (op == "+") {
        lhs = lhs->iadd(rhs);
    }
    else if (op == "-") {
        lhs = lhs->isub(rhs);
    }
    else if (op == "*") {
        lhs = lhs->imul(rhs);
    }
    else if (op == "/") {
        lhs = lhs->idiv(rhs);
    }
    else {
        ERR("bad op_type"); // shouldn't happen, but still...
    }
}
