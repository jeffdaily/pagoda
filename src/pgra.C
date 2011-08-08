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
void pgra_nonblocking(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd);
void pgra_nonblocking_groups(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd);
void pgra_nonblocking_allrec(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd);


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
            pgra_blocking(dataset, vars, writer, op, cmd);
        }

        // clean up
        delete dataset;
        delete writer;

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

            if (op == OP_RMS || op == OP_RMSSDN || op == OP_AVGSQR) {
                // square the values
                result->imul(result);
            }

            // read the rest of the records
            for (int64_t rec=1; rec<nrec; ++rec) {
                array = var->read(rec, array);
                if (op == OP_MAX) {
                    result->imax(array);
                }
                else if (op == OP_MIN) {
                    result->imin(array);
                }
                else {
                    if (op == OP_RMS || op == OP_RMSSDN || op == OP_AVGSQR) {
                        // square the values
                        array->imul(array);
                    }
                    // sum the values or squares, keep a tally
                    result->set_counter(tally);
                    result->iadd(array);
                    result->set_counter(NULL);
                }
                if (cmd.is_verbose()) {
                    pagoda::println_zero("\tfinished reading record "
                            + pagoda::to_string(rec));
                }
            }

            // normalize, multiply, etc where necessary
            if (op == OP_AVG || op == OP_SQRT || op == OP_SQRAVG
                    || op == OP_RMS || op == OP_RMS || op == OP_AVGSQR) {
                if (NULL != tally) {
                    finalize_tally(result, tally,
                            var->get_validator(0));
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
                    finalize_tally(result, tally,
                            var->get_validator(0));
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


void pgra_nonblocking(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd)
{
    vector<Variable*> record_vars;
    vector<Variable*> nonrecord_vars;
    vector<Array*> nb_arrays;
    vector<Array*> nb_results;
    vector<Array*> nb_tallys;
    Dimension *udim = dataset->get_udim();
    int64_t nrec = NULL != udim ? udim->get_size() : 0;

    Variable::split(vars, record_vars, nonrecord_vars);
    writer->icopy_vars(nonrecord_vars);
    // read/write all record variables, record-at-a-time
    // prefill/size nb_XXX vectors
    nb_arrays.assign(record_vars.size(), NULL);
    nb_tallys.assign(record_vars.size(), NULL);
    nb_results.assign(record_vars.size(), NULL);
    // first record is optimized
    for (size_t i=0; i<record_vars.size(); ++i) {
        Variable *var = record_vars[i];
        nb_results[i] = var->iread(int64_t(0));
    }
    dataset->wait();
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
        if (op == OP_RMS || op == OP_RMSSDN || op == OP_AVGSQR) {
            // square the values
            nb_results[i]->imul(nb_results[i]);
        }
    }
    // remaining records
    for (int64_t r=1; r<nrec; ++r ) {
        for (size_t i=0; i<record_vars.size(); ++i) {
            Variable *var = record_vars[i];
            nb_arrays[i] = var->iread(r, nb_arrays[i]);
        }
        dataset->wait();
        for (size_t i=0; i<record_vars.size(); ++i) {
            if (nb_results[i]->get_type() == DataType::CHAR) {
                continue;
            }
            if (op == OP_MAX) {
                nb_results[i]->imax(nb_arrays[i]);
            }
            else if (op == OP_MIN) {
                nb_results[i]->imin(nb_arrays[i]);
            }
            else {
                if (op == OP_RMS || op == OP_RMSSDN || op == OP_AVGSQR) {
                    // square the values
                    nb_arrays[i]->imul(nb_arrays[i]);
                }
                // sum the values or squares, keep a tally
                nb_results[i]->set_counter(nb_tallys[i]);
                nb_results[i]->iadd(nb_arrays[i]);
                nb_results[i]->set_counter(NULL);
            }
        }
    }
    // we can delete the temporary arrays now
    for (size_t i=0; i<record_vars.size(); ++i) {
        delete nb_arrays[i];
        nb_arrays[i] = NULL;
    }
    for (size_t i=0; i<record_vars.size(); ++i) {
        Variable *var = record_vars[i];
        // normalize, multiply, etc where necessary
        if (op == OP_AVG || op == OP_SQRT || op == OP_SQRAVG
                || op == OP_RMS || op == OP_RMS || op == OP_AVGSQR) {
            if (NULL != nb_tallys[i]) {
                finalize_tally(nb_results[i], nb_tallys[i],
                        var->get_validator(0));
                nb_results[i]->idiv(nb_tallys[i]);
            } else {
                ScalarArray scalar(nb_results[i]->get_type());
                scalar.fill_value(nrec);
                nb_results[i]->idiv(&scalar);
            }
        }
        else if (op == OP_RMSSDN) {
            if (NULL != nb_tallys[i]) {
                ScalarArray scalar(nb_tallys[i]->get_type());
                scalar.fill_value(1);
                nb_tallys[i]->isub(&scalar);
                finalize_tally(nb_results[i], nb_tallys[i],
                        var->get_validator(0));
                nb_results[i]->idiv(nb_tallys[i]);
            } else {
                ScalarArray scalar(nb_results[i]->get_type());
                scalar.fill_value(nrec-1);
                nb_results[i]->idiv(&scalar);
            }
        }

        // some operations require additional processing */
        if (op == OP_RMS || op == OP_RMSSDN || op == OP_SQRT) {
            nb_results[i]->ipow(0.5);
        }
        else if (op == OP_SQRAVG) {
            nb_results[i]->imul(nb_results[i]);
        }

        writer->iwrite(nb_results[i], var->get_name(), 0);
    }
    writer->wait();
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
    vector<Variable*> global_nonrecord_vars;
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

    // nonblocking copy of nonrecord variables
    // from global dataset to global writer
    Variable::split(vars, global_record_vars, global_nonrecord_vars);
    writer->icopy_vars(global_nonrecord_vars);

    // create a result and tally array for each variable in the dataset
    // these arrays are created on the world process group
    for (size_t i=0; i<global_record_vars.size(); ++i) {
        Variable *var = global_record_vars[i];
        string name = var->get_name();
        Array *array = var->alloc(true);
        Array *result = var->alloc(true);
        array->fill_value(0); // TODO might not need this
        result->fill_value(0); // TODO might not need this
        global_arrays.insert(make_pair(name,array));
        global_results.insert(make_pair(name,result));
        if (var->has_validator(0)) {
            Array *tally = Array::create(DataType::INT,array->get_shape());
            tally->fill_value(0); // TODO might not need this
            global_tallys.insert(make_pair(name,tally));
        }
    }

    // create the process groups
#if ROUND_ROBIN_GROUPS
    color = pagoda::me%cmd.get_number_of_groups();
#else
    color = pagoda::me/int(1.0*pagoda::npe/cmd.get_number_of_groups()+0.5);
#endif
    ASSERT(cmd.get_number_of_groups() <= pagoda::npe);
    ASSERT(color >= 0);
    group = ProcessGroup(color);

    filenames = cmd.get_input_filenames();
    ASSERT(filenames.size() >= cmd.get_number_of_groups());
    for (size_t i=0; i<filenames.size(); ++i) {
        Dataset *local_dataset;
        vector<Variable*> local_vars;
        vector<Variable*> local_record_vars;
        vector<Variable*> local_nonrecord_vars;
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
        Variable::split(local_vars, local_record_vars, local_nonrecord_vars);
        
        // create local arrays if needed
        for (size_t i=0; i<local_record_vars.size(); ++i) {
            Variable *var = local_record_vars[i];
            string name = var->get_name();
            if (local_results.count(name) == 0) {
                Array *array = var->alloc(true);
                Array *result = var->alloc(true);
                result->fill_value(0);
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
            for (size_t i=0; i<local_record_vars.size(); ++i) {
                Variable *var = local_record_vars[i];
                Array *result = local_results.find(var->get_name())->second;
                (void)var->iread(int64_t(0), result);
            }
            local_dataset->wait();
            r = 1;
            first_record = false;
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
                if (op == OP_RMS || op == OP_RMSSDN || op == OP_AVGSQR) {
                    // square the values
                    result->imul(result);
                }
            }
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
                if (op == OP_MAX) {
                    result->imax(array);
                }
                else if (op == OP_MIN) {
                    result->imin(array);
                }
                else {
                    if (op == OP_RMS || op == OP_RMSSDN || op == OP_AVGSQR) {
                        // square the values
                        array->imul(array);
                    }
                    // sum the values or squares, keep a tally
                    result->set_counter(tally);
                    result->iadd(array);
                    result->set_counter(NULL);
                }
            }
        }
    }
    // we can delete the temporary arrays now
    for (iter=local_arrays.begin(); iter!=local_arrays.end(); ++iter) {
        delete iter->second;
    }
    local_arrays.clear();

    // accumulate local results to global results
    // we copy one-group-array-at-a-time to a temporary global array and
    // perform the global accumulation
    ASSERT(local_results.size() == global_results.size());
    ASSERT(local_tallys.size() == global_tallys.size());
    ProcessGroup::set_default(ProcessGroup::get_world());
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
                // first group's array is copied directly to result
                if (i == color) {
                    if (local_result->owns_data()) {
                        vector<int64_t> lo,hi;
                        void *buf = local_result->access();
                        local_result->get_distribution(lo,hi);
                        global_result->put(buf,lo,hi);
                    }
                }
                pagoda::barrier();
                first = false;
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
                if (op == OP_MAX) {
                    global_result->imax(global_array);
                }
                else if (op == OP_MIN) {
                    global_result->imin(global_array);
                }
                else {
                    // sum the values or squares, keep a tally
                    global_result->set_counter(global_tally);
                    global_result->iadd(global_array);
                    global_result->set_counter(NULL);
                }
            }
        }
    }

    // we can delete the local results and tallys now
    for (iter=local_results.begin(); iter!=local_results.end(); ++iter) {
        delete iter->second;
    }
    for (iter=local_tallys.begin(); iter!=local_tallys.end(); ++iter) {
        delete iter->second;
    }

    for (size_t i=0; i<global_record_vars.size(); ++i) {
        Variable *var = global_record_vars[i];
        string name = var->get_name();
        Array *result = global_results.find(name)->second;
        // normalize, multiply, etc where necessary
        if (op == OP_AVG || op == OP_SQRT || op == OP_SQRAVG
                || op == OP_RMS || op == OP_RMS || op == OP_AVGSQR) {
            if (global_tallys.count(name) == 1) {
                Array *tally = global_tallys.find(name)->second;
                finalize_tally(result, tally, var->get_validator(0));
                result->idiv(tally);
            } else {
                ScalarArray scalar(result->get_type());
                scalar.fill_value(global_nrec);
                result->idiv(&scalar);
            }
        }
        else if (op == OP_RMSSDN) {
            if (global_tallys.count(name) == 1) {
                Array *tally = global_tallys.find(name)->second;
                ScalarArray scalar(tally->get_type());
                scalar.fill_value(1);
                tally->isub(&scalar);
                finalize_tally(result, tally, var->get_validator(0));
                result->idiv(tally);
            } else {
                ScalarArray scalar(result->get_type());
                scalar.fill_value(global_nrec-1);
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

        writer->iwrite(result, name, 0);
    }
    writer->wait();

    // we can delete the global results and tallys now
    for (iter=global_results.begin(); iter!=global_results.end(); ++iter) {
        delete iter->second;
    }
    for (iter=global_tallys.begin(); iter!=global_tallys.end(); ++iter) {
        delete iter->second;
    }
}


void pgra_nonblocking_allrec(Dataset *dataset, const vector<Variable*> &vars,
        FileWriter *writer, const string &op, PgraCommands &cmd)
{
    vector<Variable*> record_vars;
    vector<Variable*> nonrecord_vars;
    vector<vector<Array*> > nb_arrays;
    vector<Array*> nb_results;
    vector<Array*> nb_tallys;
    Dimension *udim = dataset->get_udim();
    int64_t nrec = NULL != udim ? udim->get_size() : 0;

    // we process record and non-record variables separately
    Variable::split(vars, record_vars, nonrecord_vars);
    // copy non-record variables unchanged to output
    writer->icopy_vars(nonrecord_vars);
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
        if (op == OP_RMS || op == OP_RMSSDN || op == OP_AVGSQR) {
            // square the values
            nb_results[v]->imul(nb_results[v]);
        }
    }
    // accumulate all records per variable
    for (size_t v=0; v<record_vars.size(); ++v) {
        for (size_t r=1; r<nrec; ++r) {
            Array *array = nb_arrays[v][r];
            if (nb_results[v]->get_type() == DataType::CHAR) {
                continue;
            }
            if (op == OP_MAX) {
                nb_results[v]->imax(array);
            }
            else if (op == OP_MIN) {
                nb_results[v]->imin(array);
            }
            else {
                if (op == OP_RMS || op == OP_RMSSDN || op == OP_AVGSQR) {
                    // square the values
                    array->imul(array);
                }
                // sum the values or squares, keep a tally
                nb_results[v]->set_counter(nb_tallys[v]);
                nb_results[v]->iadd(array);
                nb_results[v]->set_counter(NULL);
            }
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
        if (op == OP_AVG || op == OP_SQRT || op == OP_SQRAVG
                || op == OP_RMS || op == OP_RMS || op == OP_AVGSQR) {
            if (NULL != nb_tallys[v]) {
                finalize_tally(nb_results[v], nb_tallys[v],
                        var->get_validator(0));
                nb_results[v]->idiv(nb_tallys[v]);
            } else {
                ScalarArray scalar(nb_results[v]->get_type());
                scalar.fill_value(nrec);
                nb_results[v]->idiv(&scalar);
            }
        }
        else if (op == OP_RMSSDN) {
            if (NULL != nb_tallys[v]) {
                ScalarArray scalar(nb_tallys[v]->get_type());
                scalar.fill_value(1);
                nb_tallys[v]->isub(&scalar);
                finalize_tally(nb_results[v], nb_tallys[v],
                        var->get_validator(0));
                nb_results[v]->idiv(nb_tallys[v]);
            } else {
                ScalarArray scalar(nb_results[v]->get_type());
                scalar.fill_value(nrec-1);
                nb_results[v]->idiv(&scalar);
            }
        }

        // some operation results require even more additional processing
        if (op == OP_RMS || op == OP_RMSSDN || op == OP_SQRT) {
            nb_results[v]->ipow(0.5);
        }
        else if (op == OP_SQRAVG) {
            nb_results[v]->imul(nb_results[v]);
        }

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
