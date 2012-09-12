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
#include "Collectives.H"
#include "CommandException.H"
#include "CommandLineOption.H"
#include "Dataset.H"
#include "Dimension.H"
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "PgrcatCommands.H"
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


void pgrcat_blocking(PgrcatCommands &cmd);
void pgrcat_nonblocking(PgrcatCommands &cmd);
void pgrcat_nonblocking_allrec(PgrcatCommands &cmd);


int main(int argc, char **argv)
{
    PgrcatCommands cmd;
    Dataset *dataset = NULL;
    vector<Variable*> vars;
    FileWriter *writer = NULL;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);

        if (cmd.is_nonblocking()) {
            if (cmd.is_reading_all_records()) {
                pgrcat_nonblocking_allrec(cmd);
            } else {
                pgrcat_nonblocking(cmd);
            }
        } else {
            pgrcat_blocking(cmd);
        }

        // clean up
        pagoda::finalize();
    }
    catch (CommandException &ex) {
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


void pgrcat_blocking(PgrcatCommands &cmd)
{
    Dataset *dataset = NULL;
    vector<Variable*> vars;
    FileWriter *writer = NULL;
    vector<Variable*>::const_iterator var_it;

    cmd.get_inputs_and_outputs(dataset, vars, writer);

    // read/subset and write each variable...
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        Variable *var = *var_it;
        if (var->has_record()) {
            Array *array = NULL; // reuse allocated array each record
            for (int64_t rec=0,limit=var->get_nrec(); rec<limit; ++rec) {
                array = var->read(rec, array);
                ASSERT(NULL != array);
                writer->write(array, var->get_name(), rec);
            }
            delete array;
        }
        else {
            Array *array = var->read();
            ASSERT(NULL != array);
            writer->write(array, var->get_name());
            delete array;
        }
    }

    // clean up
    if (dataset) {
        delete dataset;
    }
    if (writer) {
        delete writer;
    }
}


void pgrcat_blocking_read_all(PgrcatCommands &cmd)
{
    Dataset *dataset = NULL;
    vector<Variable*> vars;
    FileWriter *writer = NULL;
    vector<Variable*>::const_iterator var_it;

    cmd.get_inputs_and_outputs(dataset, vars, writer);

    // read/subset and write each variable...
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        Variable *var = *var_it;
        Array *array = var->read();
        writer->write(array, var->get_name());
        delete array;
    }

    // clean up
    if (dataset) {
        delete dataset;
    }
    if (writer) {
        delete writer;
    }
}


void pgrcat_nonblocking(PgrcatCommands &cmd)
{
    Dataset *dataset = NULL;
    FileWriter *writer = NULL;
    vector<Variable*>::const_iterator var_it;
    vector<Variable*> vars;
    vector<Variable*> record_vars;
    vector<Variable*> nonrecord_vars;
    map<string,Array*> nb_arrays;
    Dimension *udim = NULL;
    int64_t nrec = 0;

    cmd.get_inputs_and_outputs(dataset, vars, writer);

    udim = dataset->get_udim();
    nrec = NULL != udim ? udim->get_size() : 0;

    Variable::split(vars, record_vars, nonrecord_vars);
    writer->icopy_vars(nonrecord_vars);
    // read/write all record variables record-at-a-time
    // first, set all nb_arrays to NULL
    for (size_t v=0,limit=record_vars.size(); v<limit; ++v) {
        Variable *var = record_vars[v];
        nb_arrays[var->get_name()] = NULL;
    }
    // arrays are allocated once (when r=0), then reused
    for (int64_t r=0; r<nrec; ++r) {
        for (size_t v=0,limit=record_vars.size(); v<limit; ++v) {
            Variable *var = record_vars[v];
            string name = var->get_name();
            Array *array = nb_arrays[name];
            nb_arrays[name] = var->iread(r, array);
        }
        dataset->wait();
        for (size_t v=0,limit=record_vars.size(); v<limit; ++v) {
            Variable *var = record_vars[v];
            string name = var->get_name();
            Array *array = nb_arrays[name];
            writer->iwrite(array, name, r);
        }
        writer->wait();
    }
    // delete nb_arrays
    for (map<string,Array*>::iterator it=nb_arrays.begin(), end=nb_arrays.end();
            it!=end; ++it) {
        delete it->second;
    }
    nb_arrays.clear();

    // clean up
    if (dataset) {
        delete dataset;
    }
    if (writer) {
        delete writer;
    }
}


void pgrcat_nonblocking_allrec(PgrcatCommands &cmd)
{
    Dataset *dataset = NULL;
    vector<Variable*> vars;
    FileWriter *writer = NULL;
    vector<Variable*>::const_iterator var_it;
    map<string,Array*> nb_arrays;

    cmd.get_inputs_and_outputs(dataset, vars, writer);

    // read/subset and write each variable...
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        Variable *var = *var_it;
        nb_arrays[var->get_name()] = var->iread();
    }
    dataset->wait();
    for (map<string,Array*>::iterator it=nb_arrays.begin(), end=nb_arrays.end();
            it!=end; ++it) {
        writer->iwrite(it->second, it->first);
    }
    writer->wait();
    for (map<string,Array*>::iterator it=nb_arrays.begin(), end=nb_arrays.end();
            it!=end; ++it) {
        delete it->second;
    }

    // clean up
    if (dataset) {
        delete dataset;
    }
    if (writer) {
        delete writer;
    }
}
