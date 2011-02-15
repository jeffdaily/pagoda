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
#include "Dimension.H"
#include "Error.H"
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "PgboCommands.H"
#include "Print.H"
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


static void copy(Variable *var, FileWriter *writer);
static void copy_per_record(Variable *var, FileWriter *writer);

static void op_shape_match(
    Variable *lhs, Variable *rhs, string op, FileWriter *writer);
static void op_shape_match_per_record(
    Variable *lhs, Variable *rhs, string op, FileWriter *writer);
static void op_broadcast(
    Variable *lhs, Variable *rhs, string op, FileWriter *writer);
static void op_broadcast_per_record(
    Variable *lhs, Variable *rhs, string op, FileWriter *writer);

static void do_op(Array *lhs, string op, Array *rhs);


static void pgbo_blocking(Dataset *dataset, Dataset *operand,
                          const vector<Variable*> &vars,
                          FileWriter *writer, const string &op,
                          bool per_record);
static void pgbo_nonblocking(Dataset *dataset, Dataset *operand,
                             const vector<Variable*> &vars,
                             FileWriter *writer, const string &op);


int main(int argc, char **argv)
{
    PgboCommands cmd;
    string op_type;
    Dataset *dataset;
    Dataset *operand;
    vector<Variable*> vars;
    FileWriter *writer;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);
        cmd.get_inputs_and_outputs(dataset, operand, vars, writer);
        op_type = cmd.get_operator();

        if (cmd.is_nonblocking()) {
            pgbo_nonblocking(dataset, operand, vars, writer, op_type);
        } else {
            pgbo_blocking(dataset, operand, vars, writer, op_type,
                    !cmd.is_reading_all_records());
        }

        // clean up
        delete dataset;
        delete operand;
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


void pgbo_blocking(Dataset *dataset, Dataset *operand,
                   const vector<Variable*> &vars,
                   FileWriter *writer, const string &op, bool per_record)
{
    vector<Variable*>::const_iterator var_it,var_end;

    // handle one variable at a time
    for (var_it=vars.begin(),var_end=vars.end(); var_it!=var_end; ++var_it) {
        Variable *ds_var = *var_it;
        Variable *op_var = operand->get_var(ds_var->get_name());
        if (!op_var || dataset->is_grid_var(ds_var)) {
            if (!op_var) {
                // no corresponding variable in the operand dataset,
                // so copy ds_var unchanged to output
                pagoda::println_zero(
                        "missing operand variable " + ds_var->get_name());
            }
            if (per_record && ds_var->has_record()) {
                copy_per_record(ds_var, writer);
            } else {
                copy(ds_var, writer);
            }
        }
        else if (ds_var->get_ndim() < op_var->get_ndim()) {
            ERR("operand variable rank < input variable rank");
        }
        else if (ds_var->get_shape() == op_var->get_shape()) {
            // shapes of each var match
            // if either variable is record-based, both are treated as such
            if (per_record && (ds_var->has_record() || op_var->has_record())) {
                op_shape_match_per_record(ds_var, op_var, op, writer);
            }
            else {
                // oh well, we tried to do something record-based
                op_shape_match(ds_var, op_var, op, writer);
            }
        }
        // at this point we know the shapes are mismatched, but at least they
        // are broadcast-able
        else if (per_record && ds_var->has_record()) {
            op_broadcast_per_record(ds_var, op_var, op, writer);
        } else {
            op_broadcast(ds_var, op_var, op, writer);
        }
    }
}


static void pgbo_nonblocking(Dataset *dataset, Dataset *operand,
                             const vector<Variable*> &vars,
                             FileWriter *writer, const string &op)
{
    vector<Variable*> record_vars;
    vector<Variable*> nonrecord_vars;
    vector<Variable*>::const_iterator var_it;
    vector<Variable*>::const_iterator var_end;
    vector<string> names;
    vector<Array*> nb_arrays;
    vector<Array*> nb_operands;
    Dimension *udim = dataset->get_udim();
    int64_t nrec = NULL != udim ? udim->get_size() : 0;

    Variable::split(vars, record_vars, nonrecord_vars);
    for (var_it=nonrecord_vars.begin(), var_end=nonrecord_vars.end();
            var_it!=var_end; ++var_it) {
        Variable *ds_var = *var_it;
        Variable *op_var = operand->get_var(ds_var->get_name());
        if (!op_var) {
            pagoda::println_zero("missing operand variable "
                    + ds_var->get_name());
        }
        if (ds_var->get_ndim() < op_var->get_ndim()) {
            ERR("operand variable rank < input variable rank");
        }
        nb_arrays.push_back(ds_var->iread());
        names.push_back(ds_var->get_name());
        if (dataset->is_grid_var(ds_var) || !op_var) {
            nb_operands.push_back(NULL);
        } else {
            nb_operands.push_back(op_var->iread());
        }
    }
    dataset->wait();
    operand->wait();
    for (size_t i=0,limit=nonrecord_vars.size(); i<limit; ++i) {
        Array *ds_array = nb_arrays[i];
        Array *op_array = nb_operands[i];
        string name = names[i];
        if (NULL != op_array) {
            do_op(ds_array, op, op_array);
        }
        writer->iwrite(ds_array, name);
    }
    writer->wait();
    for (size_t i=0,limit=nonrecord_vars.size(); i<limit; ++i) {
        Array *ds_array = nb_arrays[i];
        Array *op_array = nb_operands[i];
        if (NULL != op_array) {
            delete op_array;
        }
        delete ds_array;
    }
    // prefill the nb vectors with NULL so that we can reuse allocated arrays
    // this also 'clear()'s the vectors so we can reuse them
    nb_arrays.assign(record_vars.size(), NULL);
    nb_operands.assign(record_vars.size(), NULL);
    names.assign(record_vars.size(), "");

    // do the sanity checking once to avoid one message per record
    for (int64_t v=0,limit=record_vars.size(); v<limit; ++v) {
        Variable *ds_var = record_vars[v];
        Variable *op_var = operand->get_var(ds_var->get_name());
        if (!op_var) {
            pagoda::println_zero("missing operand variable "
                    + ds_var->get_name());
        }
        if (ds_var->get_ndim() < op_var->get_ndim()) {
            ERR("operand variable rank < input variable rank");
        }
    }

    // now the record variables, one record at a time
    for (int64_t rec=0; rec<nrec; ++rec) {
        for (int64_t v=0,limit=record_vars.size(); v<limit; ++v) {
            Variable *ds_var = record_vars[v];
            Variable *op_var = operand->get_var(ds_var->get_name());
            nb_arrays[v] = ds_var->iread(rec, nb_arrays[v]);
            names[v] = ds_var->get_name();
            if (dataset->is_grid_var(ds_var) || !op_var) {
                nb_operands[v] = NULL;
            } else {
                nb_operands[v] = op_var->iread(rec, nb_operands[v]);
            }
        }
        dataset->wait();
        operand->wait();
        for (size_t i=0,limit=record_vars.size(); i<limit; ++i) {
            Array *ds_array = nb_arrays[i];
            Array *op_array = nb_operands[i];
            string name = names[i];
            if (NULL != op_array) {
                do_op(ds_array, op, op_array);
            }
            writer->iwrite(ds_array, name, rec);
        }
        writer->wait();
    }
}


/**
 * Write entire Variable to FileWriter.
 */
static void copy(Variable *var, FileWriter *writer)
{
    Array *array = var->read();
    writer->write(array, var->get_name());
    delete array;
}


/**
 * Write entire Variable to FileWriter.
 */
static void copy_per_record(Variable *var, FileWriter *writer)
{
    Array *array = NULL; // reuse allocated array each record
    for (int64_t rec=0,limit=var->get_nrec(); rec<limit; ++rec) {
        array = var->read(rec, array);
        writer->write(array, var->get_name(), rec);
    }
    delete array;
}


static void op_shape_match(
    Variable *lhs, Variable *rhs, string op, FileWriter *writer)
{
    Array *lhs_array = lhs->read();
    Array *rhs_array = rhs->read();
    do_op(lhs_array, op, rhs_array);
    writer->write(lhs_array, lhs->get_name());
    delete lhs_array;
    delete rhs_array;
}


static void op_shape_match_per_record(
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


static void op_broadcast(
    Variable *lhs, Variable *rhs, string op, FileWriter *writer)
{
    Array *lhs_array = lhs->read();
    Array *rhs_array = rhs->read();
    do_op(lhs_array, op, rhs_array);
    writer->write(lhs_array, lhs->get_name());
    delete lhs_array;
    delete rhs_array;
}


static void op_broadcast_per_record(
    Variable *lhs, Variable *rhs, string op, FileWriter *writer)
{
    // It is assumed since this is a broadcasted operation, the right-hand
    // side is smaller than the left-hand side.  We read the right-hand side
    // entirely and reuse it for each record of the left-hand side.
    Array *lhs_array = NULL; // reuse allocated array each record
    Array *rhs_array = NULL;
    // special case if rhs is same shape except for the record
    if (rhs->has_record()) {
        vector<int64_t> lhs_shape = lhs->get_shape();
        vector<int64_t> rhs_shape = rhs->get_shape();
        lhs_shape.erase(lhs_shape.begin());
        rhs_shape.erase(rhs_shape.begin());
        if (lhs_shape == rhs_shape) {
            rhs_array = rhs->read(int64_t(0));
        }
    }
    if (NULL == rhs_array) {
        rhs_array = rhs->read();
    }
    for (int64_t rec=0,limit=lhs->get_nrec(); rec<limit; ++rec) {
        lhs_array = lhs->read(rec, lhs_array);
        do_op(lhs_array, op, rhs_array);
        writer->write(lhs_array, lhs->get_name(), rec);
    }
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
