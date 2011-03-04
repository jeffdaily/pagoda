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
#include "PgflintCommands.H"
#include "Print.H"
#include "ScalarArray.H"
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


#if ENABLE_INTERPOLANT
static void weights_from_interpolant(
        const string &var_name, const double &value,
        const Dataset *dataset, const Dataset *operand,
        double &weight1, double &weight2);
#endif
static void copy(Variable *var, FileWriter *writer);
static void copy_per_record(Variable *var, FileWriter *writer);
static void do_op(Variable *lhs, Variable *rhs,
        double weight1, double weight2, FileWriter *writer);
static void do_op_per_record(Variable *lhs, Variable *rhs,
        double weight1, double weight2, FileWriter *writer);
static Array* do_op(Array *lhs, Array *rhs, double weight1, double weight2,
        Array *result=NULL);

static void pgflint_blocking(Dataset *dataset, Dataset *operand,
                             double weight1, double weight2,
                             const vector<Variable*> &vars,
                             FileWriter *writer, bool per_record);
static void pgflint_nonblocking(Dataset *dataset, Dataset *operand,
                                double weight1, double weight2,
                                const vector<Variable*> &vars,
                                FileWriter *writer, bool per_record);
static void pgflint_allvars(Dataset *dataset, Dataset *operand,
                            double weight1, double weight2,
                            const vector<Variable*> &vars,
                            FileWriter *writer);


int main(int argc, char **argv)
{
    PgflintCommands cmd;
    Dataset *dataset;
    Dataset *operand;
    vector<Variable*> vars;
    FileWriter *writer;
    double w1=0.5;
    double w2=0.5;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);
        cmd.get_inputs_and_outputs(dataset, operand, vars, writer);
        w1 = cmd.get_weight1();
        w2 = cmd.get_weight2();

        if (cmd.is_reading_all_variables()) {
            pgflint_allvars(dataset, operand, w1, w2, vars, writer);
        }
        else {
            if (cmd.is_nonblocking()) {
                pgflint_nonblocking(dataset, operand, w1, w2, vars, writer,
                        !cmd.is_reading_all_records());
            }
            else {
                pgflint_blocking(dataset, operand, w1, w2, vars, writer,
                        !cmd.is_reading_all_records());
            }
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


#if ENABLE_INTERPOLANT
static void weights_from_interpolant(
        const string &var_name, const double &value,
        const Dataset *dataset, const Dataset *operand,
        double &weight1, double &weight2)
{
    Variable *ds_var = dataset->get_var(var_name);
    Variable *op_var = operand->get_var(var_name);
    double ds_buf;
    double op_buf;
    if (NULL == ds_var) {
        throw CommandException("missing interpolant in first input dataset: "
                + var_name);
    }
    if (NULL == op_var) {
        throw CommandException("missing interpolant in second input dataset: "
                + var_name);
    }
}
#endif


static void pgflint_blocking(Dataset *dataset, Dataset *operand,
                             double weight1, double weight2,
                             const vector<Variable*> &vars,
                             FileWriter *writer, bool per_record)
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
        else if (ds_var->get_shape() != op_var->get_shape()) {
            ERR("operand variable shape != input variable shape");
        }
        else {
            // shapes of each var match
            // if either variable is record-based, both are treated as such
            if (per_record && (ds_var->has_record() || op_var->has_record())) {
                do_op_per_record(ds_var, op_var, weight1, weight2, writer);
            }
            else {
                // oh well, we tried to do something record-based
                do_op(ds_var, op_var, weight1, weight2, writer);
            }
        }
    }
}


static void pgflint_nonblocking(Dataset *dataset, Dataset *operand,
                                double weight1, double weight2,
                                const vector<Variable*> &vars,
                                FileWriter *writer, bool per_record)
{
    vector<Variable*> record_vars;
    vector<Variable*> nonrecord_vars;
    vector<Variable*>::const_iterator var_it;
    vector<Variable*>::const_iterator var_end;
    vector<string> names;
    vector<Array*> nb_arrays;
    vector<Array*> nb_operands;
    vector<Array*> nb_results;
    Dimension *udim = dataset->get_udim();
    int64_t nrec = NULL != udim ? udim->get_size() : 0;

    Variable::split(vars, record_vars, nonrecord_vars);
    for (var_it=nonrecord_vars.begin(), var_end=nonrecord_vars.end();
            var_it!=var_end; ++var_it) {
        Variable *ds_var = *var_it;
        Variable *op_var = operand->get_var(ds_var->get_name());
        if (!op_var) {
            pagoda::println_zero(
                    "missing operand variable " + ds_var->get_name());
        }
        if (ds_var->get_ndim() != op_var->get_ndim()) {
            ERR("operand variable rank != input variable rank");
        }
        names.push_back(ds_var->get_name());
        nb_arrays.push_back(ds_var->iread());
        if (dataset->is_grid_var(ds_var) || !op_var) {
            nb_operands.push_back(NULL);
            nb_results.push_back(NULL);
        } else {
            nb_operands.push_back(op_var->iread());
            nb_results.push_back(
                    Array::create(DataType::DOUBLE, ds_var->get_shape()));
        }
    }
    dataset->wait();
    operand->wait();
    for (size_t i=0,limit=nonrecord_vars.size(); i<limit; ++i) {
        Array *ds_array = nb_arrays[i];
        Array *op_array = nb_operands[i];
        Array *rt_array = nb_results[i];
        string name = names[i];
        if (NULL != op_array) {
            rt_array = do_op(ds_array, op_array, weight1, weight2, rt_array);
            writer->iwrite(rt_array, name);
        } else {
            writer->iwrite(ds_array, name);
        }
    }
    writer->wait();
    for (size_t i=0,limit=nonrecord_vars.size(); i<limit; ++i) {
        Array *ds_array = nb_arrays[i];
        Array *op_array = nb_operands[i];
        Array *rt_array = nb_results[i];
        if (NULL != op_array) {
            delete op_array;
        }
        if (NULL != rt_array) {
            delete rt_array;
        }
        delete ds_array;
    }
    // prefill the nb vectors with NULL so that we can reuse allocated arrays
    // this also 'clear()'s the vectors so we can reuse them
    nb_arrays.assign(record_vars.size(), NULL);
    nb_operands.assign(record_vars.size(), NULL);
    nb_results.assign(record_vars.size(), NULL);
    names.assign(record_vars.size(), "");

    // do the sanity checking once to avoid one message per record
    for (int64_t v=0,limit=record_vars.size(); v<limit; ++v) {
        Variable *ds_var = record_vars[v];
        Variable *op_var = operand->get_var(ds_var->get_name());
        if (!op_var) {
            pagoda::println_zero(
                    "missing operand variable " + ds_var->get_name());
        }
        if (ds_var->get_ndim() != op_var->get_ndim()) {
            ERR("operand variable rank != input variable rank");
        }
    }

    if (per_record) {
        // now the record variables, one record at a time
        // special case if the operand variable is a degenerate record variable
        // or a nonrecord variable, in which case it is only read from disk once
        for (int64_t rec=0; rec<nrec; ++rec) {
            for (int64_t v=0,limit=record_vars.size(); v<limit; ++v) {
                Variable *ds_var = record_vars[v];
                Variable *op_var = operand->get_var(ds_var->get_name());
                names[v] = ds_var->get_name();
                nb_arrays[v] = ds_var->iread(rec, nb_arrays[v]);
                if (dataset->is_grid_var(ds_var) || !op_var) {
                    nb_operands[v] = NULL;
                    nb_results[v] = NULL;
                }
                else {
                    nb_operands[v] = op_var->iread(rec, nb_operands[v]);
                    nb_results[v] = Array::create(DataType::DOUBLE,
                            nb_arrays[v]->get_shape());
                }
            }
            dataset->wait();
            operand->wait();
            for (size_t i=0,limit=record_vars.size(); i<limit; ++i) {
                Array *ds_array = nb_arrays[i];
                Array *op_array = nb_operands[i];
                Array *rt_array = nb_results[i];
                string name = names[i];
                if (NULL != op_array) {
                    rt_array = do_op(ds_array, op_array, weight1, weight2,
                            rt_array);
                    writer->iwrite(rt_array, name, rec);
                } else {
                    writer->iwrite(ds_array, name, rec);
                }
            }
            writer->wait();
        }
    } else {
        for (int64_t v=0,limit=record_vars.size(); v<limit; ++v) {
            Variable *ds_var = record_vars[v];
            Variable *op_var = operand->get_var(ds_var->get_name());
            names[v] = ds_var->get_name();
            nb_arrays[v] = ds_var->iread(nb_arrays[v]);
            if (dataset->is_grid_var(ds_var) || !op_var) {
                nb_operands[v] = NULL;
                nb_results[v] = NULL;
            }
            else {
                nb_operands[v] = op_var->iread(nb_operands[v]);
                nb_results[v] = Array::create(DataType::DOUBLE,
                        nb_arrays[v]->get_shape());
            }
        }
        dataset->wait();
        operand->wait();
        for (size_t i=0,limit=record_vars.size(); i<limit; ++i) {
            Array *ds_array = nb_arrays[i];
            Array *op_array = nb_operands[i];
            Array *rt_array = nb_results[i];
            string name = names[i];
            if (NULL != op_array) {
                rt_array = do_op(ds_array, op_array, weight1, weight2,
                        rt_array);
                writer->iwrite(rt_array, name);
            } else {
                writer->iwrite(ds_array, name);
            }
        }
        writer->wait();
    }
    for (size_t i=0,limit=record_vars.size(); i<limit; ++i) {
        Array *ds_array = nb_arrays[i];
        Array *op_array = nb_operands[i];
        Array *rt_array = nb_results[i];
        if (NULL != op_array) {
            delete op_array;
        }
        if (NULL != rt_array) {
            delete rt_array;
        }
        delete ds_array;
    }
}


static void pgflint_allvars(Dataset *dataset, Dataset *operand,
                            double weight1, double weight2,
                            const vector<Variable*> &vars,
                            FileWriter *writer)
{
    vector<Variable*>::const_iterator var_it,var_end;
    vector<string> names;
    vector<Array*> nb_arrays;
    vector<Array*> nb_operands;
    vector<Array*> nb_results;

    // nonblocking read, one var at a time, all records
    for (var_it=vars.begin(),var_end=vars.end(); var_it!=var_end; ++var_it) {
        Variable *ds_var = *var_it;
        Variable *op_var = operand->get_var(ds_var->get_name());
        names.push_back(ds_var->get_name());
        nb_arrays.push_back(ds_var->iread());
        if (ds_var->get_shape() != op_var->get_shape()) {
            ERR("operand variable shape != input variable shape");
        }
        if (!op_var) {
            // no corresponding variable in the operand dataset,
            // so copy ds_var unchanged to output
            pagoda::println_zero(
                    "missing operand variable " + ds_var->get_name());
            nb_operands.push_back(NULL);
        }
        else {
            // shapes of each var match
            nb_operands.push_back(op_var->iread());
        }
    }
    dataset->wait();
    operand->wait();
    // do the operation
    for (size_t i=0,limit=nb_arrays.size(); i<limit; ++i) {
        if (NULL == nb_operands[i]) {
            nb_results.push_back(
                    do_op(nb_arrays[i], nb_operands[i], weight1, weight2));
        }
        else {
            nb_results.push_back(NULL);
        }
    }
    // nonblocking write, one var at a time, all records
    for (size_t i=0,limit=nb_arrays.size(); i<limit; ++i) {
        if (NULL == nb_results[i]) {
            writer->iwrite(nb_arrays[i], names[i]);
        }
        else {
            writer->iwrite(nb_results[i], names[i]);
        }
    }
    writer->wait();
    // clean up
    for (size_t i=0,limit=nb_arrays.size(); i<limit; ++i) {
        if (NULL != nb_operands[i]) {
            delete nb_operands[i];
        }
        if (NULL != nb_results[i]) {
            delete nb_results[i];
        }
        delete nb_arrays[i];
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


static void do_op(Variable *lhs, Variable *rhs,
        double weight1, double weight2, FileWriter *writer)
{
    Array *lhs_array = lhs->read();
    Array *rhs_array = rhs->read();
    Array *result = NULL;
    result = do_op(lhs_array, rhs_array, weight1, weight2);
    writer->write(result, lhs->get_name());
    delete lhs_array;
    delete rhs_array;
    delete result;
}


static void do_op_per_record(Variable *lhs, Variable *rhs,
        double weight1, double weight2, FileWriter *writer)
{
    // operate one record at a time for both vars
    Array *lhs_array = NULL; // reuse allocated array each record
    Array *rhs_array = NULL; // reuse allocated array each record
    Array *result = NULL; // reuse allocated array each record
    for (int64_t rec=0,limit=lhs->get_nrec(); rec<limit; ++rec) {
        lhs_array = lhs->read(rec, lhs_array);
        rhs_array = rhs->read(rec, rhs_array);
        result = do_op(lhs_array, rhs_array, weight1, weight2, result);
        writer->write(result, lhs->get_name(), rec);
    }
    delete lhs_array;
    delete rhs_array;
    delete result;
}


static Array* do_op(Array *lhs, Array *rhs, double weight1, double weight2,
        Array *result)
{
    ScalarArray array_weight1(DataType::DOUBLE);
    ScalarArray array_weight2(DataType::DOUBLE);

    if (NULL == result) {
        result = Array::create(DataType::DOUBLE, lhs->get_shape());
    }
    array_weight1.fill_value(weight1);
    array_weight2.fill_value(weight2);
    result->fill_value(double(0.0));
    lhs->imul(&array_weight1);
    rhs->imul(&array_weight2);
    result->iadd(lhs);
    result->iadd(rhs);
    return result;
}
