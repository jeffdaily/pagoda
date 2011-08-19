#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "Array.H"
#include "Bootstrap.H"
#include "CommandException.H"
#include "CommandLineOption.H"
#include "Dataset.H"
#include "Dimension.H"
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "PgraCommands.H"
#include "Print.H"
#include "ScalarArray.H"
#include "Util.H"
#include "Validator.H"
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


void initialize_tally(Array *result, Array *tally, Validator *validator);
void finalize_tally(Array *result, Array *tally, Validator *validator);

void pgra_blocking(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd);
void pgra_blocking_groups(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd);
void pgra_nonblocking(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd);
void pgra_nonblocking_groups(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd);
void pgra_nonblocking_allrec(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd);

void reduce(const string &op, Array *result, Array *array, Array *tally, bool needs_square=true);


static int get_color(int num_groups)
{
#if ROUND_ROBIN_GROUPS
    return pagoda::me%num_groups;
#else
    return pagoda::me/int(1.0*pagoda::npe/num_groups+0.5);
#endif
}


static void delete_arrays(map<string,Array*> &arrays)
{
    map<string,Array*>::iterator iter;
    for (iter=arrays.begin(); iter!=arrays.end(); ++iter) {
        delete iter->second;
    }
    arrays.clear();
}


int main(int argc, char **argv)
{
    PgraCommands cmd;
    Dataset *dataset = NULL;
    vector<Variable*> vars;
    FileWriter *writer = NULL;
    string op;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);
        cmd.get_inputs_and_outputs(dataset, vars, writer);
        op = cmd.get_operator();

        if (cmd.is_nonblocking()) {
            if (cmd.is_reading_all_records()) {
                pgra_nonblocking_allrec(dataset, vars, writer, op, cmd);
            } else {
                if (cmd.get_number_of_groups() > 1) {
                    pgra_nonblocking_groups(dataset, vars, writer, op, cmd);
                } else {
                    pgra_nonblocking(dataset, vars, writer, op, cmd);
                }
            }
        } else {
            if (cmd.get_number_of_groups() > 1) {
                pgra_blocking_groups(dataset, vars, writer, op, cmd);
            } else {
                pgra_blocking(dataset, vars, writer, op, cmd);
            }
        }

        // clean up
        if (dataset) {
            delete dataset;
        }
        if (writer) {
            delete writer;
        }

        pagoda::finalize();
    }
    catch (CommandException &ex) {
        if (dataset) {
            delete dataset;
        }
        if (writer) {
            delete writer;
        }
        pagoda::println_zero(ex.what());
        pagoda::finalize();
        return EXIT_FAILURE;
    }
    catch (PagodaException &ex) {
        pagoda::abort(ex.what());
    }
    catch (exception &ex) {
        pagoda::abort(ex.what());
    }

    return EXIT_SUCCESS;
}


void maybe_square(const string &op, Array *result)
{
    if (op == OP_RMS || op == OP_RMSSDN || op == OP_AVGSQR) {
        // square the values
        result->imul(result);
    }
}


void reduce(const string &op, Array *result, Array *array, Array *tally, bool needs_square)
{
    if (op == OP_MAX) {
        result->imax(array);
    }
    else if (op == OP_MIN) {
        result->imin(array);
    }
    else {
        if (needs_square) {
            maybe_square(op, array);
        }
        // sum the values or squares, keep a tally
        result->set_counter(tally);
        result->iadd(array);
        result->set_counter(NULL);
    }
}


static void finalize_avg(
        const string &op, Variable *var, Array *result,
        Array *tally, int64_t nrec)
{
    // normalize, multiply, etc where necessary
    if (op == OP_AVG || op == OP_SQRT || op == OP_SQRAVG
            || op == OP_RMS || op == OP_RMS || op == OP_AVGSQR) {
        if (NULL != tally) {
            finalize_tally(result, tally, var->get_validator(0));
            result->idiv(tally);
        } else {
            ScalarArray scalar(result->get_type());
            scalar.fill_value(nrec);
            result->idiv(&scalar);
        }
    }
    else if (op == OP_RMSSDN) {
        if (NULL != tally) {
            ScalarArray scalar(tally->get_type());
            scalar.fill_value(1);
            tally->isub(&scalar);
            finalize_tally(result, tally, var->get_validator(0));
            result->idiv(tally);
        } else {
            ScalarArray scalar(result->get_type());
            scalar.fill_value(nrec-1);
            result->idiv(&scalar);
        }
    }
    // some operations require additional processing */
    if (op == OP_RMS || op == OP_RMSSDN || op == OP_SQRT) {
        result->ipow(0.5);
    }
    else if (op == OP_SQRAVG) {
        result->imul(result);
    }
}


void pgra_blocking(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd)
{
    vector<Variable*>::const_iterator var_it;

    // read each variable in order
    // for record variables, read one record at a time
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        Variable *var = *var_it;
        if (cmd.is_verbose()) {
            pagoda::println_zero("processing variable: " + var->get_name());
        }
        if (var->has_record() && var->get_nrec() > 0) {
            int64_t nrec = var->get_nrec();
            Array *result = NULL;
            Array *array = NULL; // reuse allocated array each record
            Array *tally = NULL;

            // optimization: read the first record directly into result
            result = var->read(0, result);
            if (cmd.is_verbose()) {
                pagoda::println_zero("\tfinished reading record 0");
            }

            // ignore character data
            if (result->get_type() == DataType::CHAR) {
                writer->write(result, var->get_name(), 0);
                delete result;
                continue;
            }

            if (var->has_validator(0)) {
                tally = Array::create(DataType::INT, result->get_shape());
                initialize_tally(result, tally, var->get_validator(0));
            }

            maybe_square(op, result);

            // read the rest of the records
            for (int64_t rec=1; rec<nrec; ++rec) {
                array = var->read(rec, array);
                reduce(op, result, array, tally);
                if (cmd.is_verbose()) {
                    pagoda::println_zero("\tfinished reading record "
                            + pagoda::to_string(rec));
                }
            }

            // normalize, multiply, etc where necessary
            finalize_avg(op, var, result, tally, nrec);

            writer->write(result, var->get_name(), 0);
            if (cmd.is_verbose()) {
                pagoda::println_zero("\tfinished writing");
            }
            delete result;
            delete array;
            if (NULL != tally) {
                delete tally;
            }
        }
        else {
            Array *array = var->read();
            ASSERT(NULL != array);
            writer->write(array, var->get_name());
            delete array;
        }
    }
}


void pgra_blocking_groups(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd)
{
    vector<string> filenames;
    map<string,Array*> global_arrays;
    map<string,Array*> global_results;
    map<string,Array*> global_tallys;
    map<string,Array*> local_arrays;
    map<string,Array*> local_results;
    map<string,Array*> local_tallys;
    map<string,Array*>::iterator iter;
    Dimension *global_udim;
    int64_t global_nrec;
    int color;
    bool first_record = true;
    ProcessGroup group; // defaults to world group

    global_udim = dataset->get_udim();
    global_nrec = (NULL != global_udim) ? global_udim->get_size() : 0;

    // create the process groups
    if (cmd.is_verbose()) {
        pagoda::print_zero("creating "
                + pagoda::to_string(cmd.get_number_of_groups())
                + " process groups...");
    }
    color = get_color(cmd.get_number_of_groups());
    ASSERT(cmd.get_number_of_groups() <= pagoda::npe);
    ASSERT(color >= 0);
    group = ProcessGroup(color);
    if (cmd.is_verbose()) {
        pagoda::println_zero("done");
    }

    if (cmd.is_verbose()) {
        pagoda::print_zero("reading input files, locally accumulating...");
    }
    filenames = cmd.get_input_filenames();
    ASSERT(filenames.size() >= cmd.get_number_of_groups());
    for (size_t i=0; i<filenames.size(); ++i) {
        Dataset *local_dataset;
        vector<Variable*> local_vars;

        if (i%cmd.get_number_of_groups() != color) {
            continue; // skip files to be handled by another group
        }

        local_dataset = Dataset::open(filenames[i], group);
        local_vars = cmd.get_variables(local_dataset);
        
        // read each variable in order
        // for record variables, read one record at a time
        for (size_t i=0; i<local_vars.size(); ++i) {
            Variable *var = local_vars[i];
            string name = var->get_name();
            if (var->has_record() && var->get_nrec() > 0) {
                Array *local_result = NULL;
                Array *local_array = NULL;
                Array *local_tally = NULL;
                int64_t rec = 0;
                int64_t local_nrec = var->get_nrec();

                // read directly into local_result if it doesn't already exist
                if (local_results.count(name) == 0) {
                    rec = 1;
                    local_result = var->read(0, local_result);
                    local_results.insert(make_pair(name, local_result));
                    // ignore character data
                    if (local_result->get_type() == DataType::CHAR) {
                        continue;
                    }
                    if (var->has_validator(0)) {
                        local_tally = Array::create(
                                DataType::INT, local_result->get_shape());
                        initialize_tally(local_result, local_tally,
                                var->get_validator(0));
                        local_tallys.insert(make_pair(name, local_tally));
                    }
                    maybe_square(op, local_result);
                } else {
                    local_result = local_results.find(name)->second;
                    if (local_tallys.count(name) == 1) {
                        local_tally = local_tallys.find(name)->second;
                    }
                    if (local_arrays.count(name) == 1) {
                        local_array = local_arrays.find(name)->second;
                    } else {
                        local_array = var->alloc(true);
                        local_arrays.insert(make_pair(name, local_array));
                    }
                }
                // read the rest of the records
                for (/*empty*/; rec<local_nrec; ++rec) {
                    local_array = var->read(rec, local_array);
                    reduce(op, local_result, local_array, local_tally);
                }
                // normalize, multiply, etc where necessary
                finalize_avg(op, var, local_result, local_tally, local_nrec);
            } else {
                // only read the fixed variable if we haven't already
                if (local_results.count(name) == 0) {
                    Array *array = var->read();
                    local_results.insert(make_pair(name, array));
                }
            }
        }
    }
    if (cmd.is_verbose()) {
        pagoda::println_zero("done");
    }
    if (cmd.is_verbose()) {
        pagoda::print_zero("cleaning up temporary local buffers...");
    }
    // we can delete the temporary local_arrays now
    delete_arrays(local_arrays);
    if (cmd.is_verbose()) {
        pagoda::println_zero("done");
    }

    ProcessGroup::set_default(ProcessGroup::get_world());

    if (cmd.is_verbose()) {
        pagoda::print_zero("creating global buffers...");
    }
    // create a result and tally array for each variable in the dataset
    // these arrays are created on the world process group
    for (size_t i=0; i<vars.size(); ++i) {
        Variable *var = vars[i];
        string name = var->get_name();
        Array *array = NULL;
        Array *result = NULL;
        if (var->has_record() && var->get_nrec() > 0) {
            array = var->alloc(true);
            result = var->alloc(true);
        } else {
            array = var->alloc(false);
            result = var->alloc(false);
        }
        global_arrays.insert(make_pair(name, array));
        global_results.insert(make_pair(name, result));
        if (var->has_validator(0)) {
            Array *tally = Array::create(DataType::INT,array->get_shape());
            global_tallys.insert(make_pair(name, tally));
        }
    }
    if (cmd.is_verbose()) {
        pagoda::println_zero("done");
    }

    if (cmd.is_verbose()) {
        pagoda::print_zero("globally accumulating results...");
    }
    // accumulate local results to global results
    // we copy one-group-array-at-a-time to a temporary global array and
    // perform the global accumulation
    //ASSERT(local_results.size() == global_results.size());
    //ASSERT(local_tallys.size() == global_tallys.size());
    for (size_t i=0; i<vars.size(); ++i) {
        Variable *var = vars[i];
        string name = var->get_name();
        Array *global_result = global_results.find(name)->second;
        Array *global_array = global_arrays.find(name)->second;
        Array *global_tally = NULL;
        Array *local_result = local_results.find(name)->second;
        Array *local_tally = NULL;
        bool first = true;

        // TODO TODO TODO!!accumulate the global_tally!!
        if (global_tallys.count(name) == 1) {
            global_tally = global_tallys.find(name)->second;
        }
        if (local_tallys.count(name) == 1) {
            local_tally = local_tallys.find(name)->second;
        }

        for (int i=0; i<cmd.get_number_of_groups(); ++i) {
            if (first) {
                first = false;
                // first group's array is copied directly to result
                if (i == color) {
                    if (local_result->owns_data()) {
                        vector<int64_t> lo,hi;
                        void *buf = local_result->access();
                        local_result->get_distribution(lo,hi);
                        global_result->put(buf,lo,hi);
                    }
                }
                //pagoda::barrier(); // this one isn't needed
                continue;
            } else if (var->has_record() && var->get_nrec() > 0) {
                // subsequent groups' arrays are copied and modified
                // but only for record variables
                if (i == color) {
                    if (local_result->owns_data()) {
                        vector<int64_t> lo,hi;
                        void *buf = local_result->access();
                        local_result->get_distribution(lo,hi);
                        global_array->put(buf,lo,hi);
                    }
                }
                pagoda::barrier();
                reduce(op, global_result, global_array, global_tally, false);
            }
        }
    }
    if (cmd.is_verbose()) {
        pagoda::println_zero("done");
    }

    // we can delete the local results, local tallys, and global arrays now
    delete_arrays(local_results);
    delete_arrays(local_tallys);
    delete_arrays(global_arrays);

    if (cmd.is_verbose()) {
        pagoda::print_zero("finalizing results...");
    }
    for (size_t i=0; i<vars.size(); ++i) {
        Variable *var = vars[i];
        string name = var->get_name();
        Array *result = global_results.find(name)->second;
        Array *tally = NULL;

        if (var->has_record() && var->get_nrec() > 0) {
            if (global_tallys.count(name) == 1) {
                Array *tally = global_tallys.find(name)->second;
            }
            // normalize, multiply, etc where necessary
            finalize_avg(op, var, result, tally, global_nrec);
        }
    }
    if (cmd.is_verbose()) {
        pagoda::println_zero("done");
    }
    if (cmd.is_verbose()) {
        pagoda::print_zero("writing results...");
    }
    for (size_t i=0; i<vars.size(); ++i) {
        Variable *var = vars[i];
        string name = var->get_name();
        Array *result = global_results.find(name)->second;

        if (var->has_record() && var->get_nrec() > 0) {
            writer->write(result, name, 0);
        } else {
            writer->write(result, name);
        }
    }
    if (cmd.is_verbose()) {
        pagoda::println_zero("done");
    }

    // we can delete the global results and tallys now
    delete_arrays(global_results);
    delete_arrays(global_tallys);
}


void pgra_nonblocking(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd)
{
    vector<Variable*> record_vars;
    vector<Variable*> fixed_vars;
    vector<Array*> nb_arrays;
    vector<Array*> nb_results;
    vector<Array*> nb_tallys;
    Dimension *udim = dataset->get_udim();
    int64_t nrec = NULL != udim ? udim->get_size() : 0;

    if (cmd.is_verbose()) {
        pagoda::print_zero("copying fixed variables...");
    }
    Variable::split(vars, record_vars, fixed_vars);
    writer->icopy_vars(fixed_vars);
    if (cmd.is_verbose()) {
        pagoda::println_zero("done");
    }

    // read/write all record variables, record-at-a-time
    // prefill/size nb_XXX vectors
    nb_arrays.assign(record_vars.size(), NULL);
    nb_tallys.assign(record_vars.size(), NULL);
    nb_results.assign(record_vars.size(), NULL);
    // first record is optimized
    if (cmd.is_verbose()) {
        pagoda::print_zero("reading record 0...");
    }
    for (size_t i=0; i<record_vars.size(); ++i) {
        Variable *var = record_vars[i];
        nb_results[i] = var->iread(int64_t(0));
    }
    dataset->wait();
    if (cmd.is_verbose()) {
        pagoda::println_zero("done");
    }
    for (size_t i=0; i<record_vars.size(); ++i) {
        Variable *var = record_vars[i];
        if (nb_results[i]->get_type() == DataType::CHAR) {
            continue;
        }
        if (var->has_validator(0)) {
            nb_tallys[i] = Array::create(
                    DataType::INT, nb_results[i]->get_shape());
            initialize_tally(nb_results[i], nb_tallys[i],
                    var->get_validator(0));
        }
        maybe_square(op, nb_results[i]);
    }
    if (cmd.is_verbose()) {
        pagoda::println_zero("finished processing record 0");
    }
    // remaining records
    for (int64_t r=1; r<nrec; ++r ) {
        if (cmd.is_verbose()) {
            pagoda::print_zero("reading record " + pagoda::to_string(r)
                    + "...");
        }
        for (size_t i=0; i<record_vars.size(); ++i) {
            Variable *var = record_vars[i];
            nb_arrays[i] = var->iread(r, nb_arrays[i]);
        }
        dataset->wait();
        if (cmd.is_verbose()) {
            pagoda::println_zero("done");
        }
        for (size_t i=0; i<record_vars.size(); ++i) {
            if (nb_results[i]->get_type() == DataType::CHAR) {
                continue;
            }
            reduce(op, nb_results[i], nb_arrays[i], nb_tallys[i]);
        }
        if (cmd.is_verbose()) {
            pagoda::println_zero("finished processing record "
                    + pagoda::to_string(r));
        }
    }
    // we can delete the temporary arrays now
    for (size_t i=0; i<record_vars.size(); ++i) {
        delete nb_arrays[i];
        nb_arrays[i] = NULL;
    }
    for (size_t i=0; i<record_vars.size(); ++i) {
        Variable *var = record_vars[i];
        if (cmd.is_verbose()) {
            pagoda::println_zero("final procesing for variable "
                    + var->get_name());
        }
        // normalize, multiply, etc where necessary
        finalize_avg(op, var, nb_results[i], nb_tallys[i], nrec);

        writer->iwrite(nb_results[i], var->get_name(), 0);
    }
    if (cmd.is_verbose()) {
        pagoda::println_zero("before final write");
    }
    writer->wait();
    if (cmd.is_verbose()) {
        pagoda::println_zero(" after final write");
    }
    // clean up
    for (size_t i=0; i<record_vars.size(); ++i) {
        delete nb_results[i];
        nb_results[i] = NULL;
        if (NULL != nb_tallys[i]) {
            delete nb_tallys[i];
            nb_tallys[i] = NULL;
        }
    }
}


// TODO optimize the "global" dataset
// i.e. perhaps we don't need to open the dataset globally
// so that we avoid opening all of the files twice
void pgra_nonblocking_groups(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd)
{
    vector<string> filenames;
    vector<Variable*> global_record_vars;
    vector<Variable*> global_fixed_vars;
    map<string,Array*> global_arrays;
    map<string,Array*> global_results;
    map<string,Array*> global_tallys;
    map<string,Array*> local_arrays;
    map<string,Array*> local_results;
    map<string,Array*> local_tallys;
    map<string,Array*>::iterator iter;
    Dimension *global_udim;
    int64_t global_nrec;
    int color;
    bool first_record = true;
    ProcessGroup group; // defaults to world group

    global_udim = dataset->get_udim();
    global_nrec = (NULL != global_udim) ? global_udim->get_size() : 0;

    // nonblocking copy of fixed variables
    // from global dataset to global writer
    if (cmd.is_verbose()) {
        pagoda::print_zero("copying fixed variables...");
    }
    Variable::split(vars, global_record_vars, global_fixed_vars);
    writer->icopy_vars(global_fixed_vars);
    if (cmd.is_verbose()) {
        pagoda::println_zero("done");
    }

    // create the process groups
    if (cmd.is_verbose()) {
        pagoda::print_zero("creating "
                + pagoda::to_string(cmd.get_number_of_groups())
                + " process groups...");
    }
    color = get_color(cmd.get_number_of_groups());
    ASSERT(cmd.get_number_of_groups() <= pagoda::npe);
    ASSERT(color >= 0);
    group = ProcessGroup(color);
    if (cmd.is_verbose()) {
        pagoda::println_zero("done");
    }

    filenames = cmd.get_input_filenames();
    ASSERT(filenames.size() >= cmd.get_number_of_groups());
    for (size_t i=0; i<filenames.size(); ++i) {
        Dataset *local_dataset;
        vector<Variable*> local_vars;
        vector<Variable*> local_record_vars;
        vector<Variable*> local_fixed_vars;
        Dimension *local_udim;
        int64_t local_nrec;
        int64_t r = 0;

        if (i%cmd.get_number_of_groups() != color) {
            continue; // skip files to be handled by another group
        }

        local_dataset = Dataset::open(filenames[i], group);
        local_udim = local_dataset->get_udim();
        local_nrec = (NULL != local_udim) ? local_udim->get_size() : 0;
        local_vars = cmd.get_variables(local_dataset);
        Variable::split(local_vars, local_record_vars, local_fixed_vars);
        
        // create local arrays if needed
        for (size_t i=0; i<local_record_vars.size(); ++i) {
            Variable *var = local_record_vars[i];
            string name = var->get_name();
            if (local_results.count(name) == 0) {
                Array *array = var->alloc(true);
                Array *result = var->alloc(true);
                //result->fill_value(0); // TODO might not need this
                if (var->has_validator(0)) {
                    Array *tally = Array::create(
                            DataType::INT, array->get_shape());
                    tally->fill_value(0);
                    local_tallys.insert(make_pair(name, tally));
                }
                local_arrays.insert(make_pair(name, array));
                local_results.insert(make_pair(name, result));
            }
        }

        // first record is optimized
        if (first_record) {
            first_record = false;
            for (size_t i=0; i<local_record_vars.size(); ++i) {
                Variable *var = local_record_vars[i];
                Array *result = local_results.find(var->get_name())->second;
                (void)var->iread(int64_t(0), result);
            }
            local_dataset->wait();
            for (size_t i=0; i<local_record_vars.size(); ++i) {
                Variable *var = local_record_vars[i];
                Array *result = local_results.find(var->get_name())->second;
                Array *tally = NULL;

                if (result->get_type() == DataType::CHAR) {
                    continue;
                }
                if (var->has_validator(0)
                        && local_tallys.count(var->get_name())) {
                    tally = local_tallys.find(var->get_name())->second;
                    initialize_tally(result, tally, var->get_validator(0));
                }
                maybe_square(op, result);
            }
            r = 1;
        } else {
            r = 0;
        }

        // remaining records
        for (/*empty*/; r<local_nrec; ++r ) {
            for (size_t i=0; i<local_record_vars.size(); ++i) {
                Variable *var = local_record_vars[i];
                Array *array = local_arrays.find(var->get_name())->second;
                (void)var->iread(r, array);
            }
            local_dataset->wait();
            for (size_t i=0; i<local_record_vars.size(); ++i) {
                Variable *var = local_record_vars[i];
                Array *array = local_arrays.find(var->get_name())->second;
                Array *result = local_results.find(var->get_name())->second;
                Array *tally = NULL;
                if (local_tallys.count(var->get_name())) {
                    tally = local_tallys.find(var->get_name())->second;
                }
                if (result->get_type() == DataType::CHAR) {
                    continue;
                }
                reduce(op, result, array, tally);
            }
        }
    }
    // we can delete the temporary local arrays now
    delete_arrays(local_arrays);

    ProcessGroup::set_default(ProcessGroup::get_world());

    // create a result and tally array for each variable in the dataset
    // these arrays are created on the world process group
    for (size_t i=0; i<global_record_vars.size(); ++i) {
        Variable *var = global_record_vars[i];
        string name = var->get_name();
        Array *array = var->alloc(true);
        Array *result = var->alloc(true);
        //array->fill_value(0); // TODO might not need this
        //result->fill_value(0); // TODO might not need this
        global_arrays.insert(make_pair(name,array));
        global_results.insert(make_pair(name,result));
        if (var->has_validator(0)) {
            Array *tally = Array::create(DataType::INT,array->get_shape());
            tally->fill_value(0); // TODO might not need this
            global_tallys.insert(make_pair(name,tally));
        }
    }

    if (cmd.is_verbose()) {
        pagoda::print_zero("accumulating results...");
    }
    // accumulate local results to global results
    // we copy one-group-array-at-a-time to a temporary global array and
    // perform the global accumulation
    ASSERT(local_results.size() == global_results.size());
    ASSERT(local_tallys.size() == global_tallys.size());
    for (iter=global_results.begin(); iter!=global_results.end(); ++iter) {
        string name = iter->first;
        Array *global_result = iter->second;
        Array *global_array = global_arrays.find(name)->second;
        Array *global_tally = NULL;
        Array *local_result = local_results.find(name)->second;
        bool first = true;

        // TODO TODO TODO!!accumulate the global_tally!!
        if (global_tallys.count(name) == 1) {
            global_tally = global_tallys.find(name)->second;
        }
        for (int i=0; i<cmd.get_number_of_groups(); ++i) {
            if (first) {
                first = false;
                // first group's array is copied directly to result
                if (i == color) {
                    if (local_result->owns_data()) {
                        vector<int64_t> lo,hi;
                        void *buf = local_result->access();
                        local_result->get_distribution(lo,hi);
                        global_result->put(buf,lo,hi);
                    }
                }
                //pagoda::barrier(); // this one isn't needed
                continue;
            } else {
                // subsequent groups' arrays are copied and modified
                if (i == color) {
                    if (local_result->owns_data()) {
                        vector<int64_t> lo,hi;
                        void *buf = local_result->access();
                        local_result->get_distribution(lo,hi);
                        global_array->put(buf,lo,hi);
                    }
                }
                pagoda::barrier();
                reduce(op, global_result, global_array, global_tally, false);
            }
        }
    }
    if (cmd.is_verbose()) {
        pagoda::println_zero("done");
    }

    // we can delete the local results, local tallys, and global arrays now
    delete_arrays(local_results);
    delete_arrays(local_tallys);
    delete_arrays(global_arrays);

    for (size_t i=0; i<global_record_vars.size(); ++i) {
        Variable *var = global_record_vars[i];
        string name = var->get_name();
        Array *result = global_results.find(name)->second;
        Array *tally = NULL;
        if (global_tallys.count(name) == 1) {
            Array *tally = global_tallys.find(name)->second;
        }
        // normalize, multiply, etc where necessary
        finalize_avg(op, var, result, tally, global_nrec);

        writer->iwrite(result, name, 0);
    }
    if (cmd.is_verbose()) {
        pagoda::print_zero("writing results...");
    }
    writer->wait();
    if (cmd.is_verbose()) {
        pagoda::println_zero("done");
    }

    // we can delete the global results and tallys now
    delete_arrays(global_results);
    delete_arrays(global_tallys);
}


void pgra_nonblocking_allrec(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd)
{
    vector<Variable*> record_vars;
    vector<Variable*> fixed_vars;
    vector<vector<Array*> > nb_arrays;
    vector<Array*> nb_results;
    vector<Array*> nb_tallys;
    Dimension *udim = dataset->get_udim();
    int64_t nrec = NULL != udim ? udim->get_size() : 0;

    // we process record and fixed variables separately
    Variable::split(vars, record_vars, fixed_vars);
    // copy fixed variables unchanged to output
    writer->icopy_vars(fixed_vars);
    // prefill/size nb_XXX vectors
    nb_arrays.assign(record_vars.size(), vector<Array*>(nrec, 0));
    nb_tallys.assign(record_vars.size(), NULL);
    nb_results.assign(record_vars.size(), NULL);
    // read all records from all record variables
    // first record read directly into result array
    for (size_t v=0; v<record_vars.size(); ++v) {
        Variable *var = record_vars[v];
        nb_results[v] = var->iread(int64_t(0));
    }
    // remaining records read into temporary buffers
    for (int64_t r=1; r<nrec; ++r ) {
        for (size_t v=0; v<record_vars.size(); ++v) {
            Variable *var = record_vars[v];
            nb_arrays[v][r] = var->iread(r);
        }
    }
    dataset->wait();
    // create and initialize tally arrays
    for (size_t v=0; v<record_vars.size(); ++v) {
        Variable *var = record_vars[v];
        if (nb_results[v]->get_type() == DataType::CHAR) {
            continue;
        }
        if (var->has_validator(0)) {
            nb_tallys[v] = Array::create(
                    DataType::INT, nb_results[v]->get_shape());
            initialize_tally(nb_results[v], nb_tallys[v],
                    var->get_validator(0));
        }
        maybe_square(op, nb_results[v]);
    }
    // accumulate all records per variable
    for (size_t v=0; v<record_vars.size(); ++v) {
        for (size_t r=1; r<nrec; ++r) {
            Array *array = nb_arrays[v][r];
            if (nb_results[v]->get_type() == DataType::CHAR) {
                continue;
            }
            reduce(op, nb_results[v], array, nb_tallys[v]);
        }
    }
    // we can delete the temporary arrays now
    for (size_t v=0; v<record_vars.size(); ++v) {
        for (size_t r=1; r<nrec; ++r) {
            delete nb_arrays[v][r];
            nb_arrays[v][r] = NULL;
        }
    }
    // some operation results require additional processing
    // also write the results
    for (size_t v=0; v<record_vars.size(); ++v) {
        Variable *var = record_vars[v];
        // normalize, multiply, etc where necessary
        finalize_avg(op, var, nb_results[v], nb_tallys[v], nrec);

        writer->iwrite(nb_results[v], var->get_name(), 0);
    }
    writer->wait();
    // clean up
    for (size_t v=0; v<record_vars.size(); ++v) {
        delete nb_results[v];
        nb_results[v] = NULL;
        if (NULL != nb_tallys[v]) {
            delete nb_tallys[v];
            nb_tallys[v] = NULL;
        }
    }
}


void initialize_tally(Array *result, Array *tally, Validator *validator)
{
    void *ptr_result;
    
    // result and tally arrays are assumed to have same distributions
    ASSERT(result->same_distribution(tally));

    ptr_result = result->access();
    if (NULL != ptr_result) {
        int *buf_tally = static_cast<int*>(tally->access());
        DataType type = result->get_type();
        ASSERT(NULL != buf_tally);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            T *buf_result = static_cast<T*>(ptr_result); \
            for (int64_t i=0,limit=result->get_local_size(); i<limit; ++i) { \
                if (validator->is_valid(&buf_result[i])) { \
                    buf_tally[i] = 1; \
                } else { \
                    buf_tally[i] = 0; \
                    buf_result[i] = 0; \
                } \
            } \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
        result->release();
        tally->release_update();
    }

    delete validator;
}


void finalize_tally(Array *result, Array *tally, Validator *validator)
{
    void *ptr_result;
    
    // result and tally arrays are assumed to have same distributions
    ASSERT(result->same_distribution(tally));

    ptr_result = result->access();
    if (NULL != ptr_result) {
        int *buf_tally = static_cast<int*>(tally->access());
        DataType type = result->get_type();
        ASSERT(NULL != buf_tally);
#define DATATYPE_EXPAND(DT,T) \
        if (DT == type) { \
            T *buf_result = static_cast<T*>(ptr_result); \
            const T fill_value = *static_cast<const T*>(validator->get_fill_value());\
            for (int64_t i=0,limit=result->get_local_size(); i<limit; ++i) { \
                if (buf_tally[i] <= 0) { \
                    buf_tally[i] = 1; \
                    buf_result[i] = fill_value; \
                } \
            } \
        } else
#include "DataType.def"
        {
            EXCEPT(DataTypeException, "DataType not handled", type);
        }
        result->release_update();
        tally->release_update();
    }

    delete validator;
}
