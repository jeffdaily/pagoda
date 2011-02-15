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
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "Print.H"
#include "SubsetterCommands.H"
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


void pgsub_blocking(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer);
void pgsub_blocking_read_all(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer);
void pgsub_nonblocking(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer);
void pgsub_nonblocking_read_all(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer);


int main(int argc, char **argv)
{
    SubsetterCommands cmd;
    Dataset *dataset;
    vector<Variable*> vars;
    FileWriter *writer;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);
        cmd.get_inputs_and_outputs(dataset, vars, writer);

        if (cmd.is_reading_all_records()) {
            if (cmd.is_nonblocking()) {
                pgsub_nonblocking_read_all(dataset, vars, writer);
            } else {
                pgsub_blocking_read_all(dataset, vars, writer);
            }
        } else {
            if (cmd.is_nonblocking()) {
                pgsub_nonblocking(dataset, vars, writer);
            } else {
                pgsub_blocking(dataset, vars, writer);
            }
        }

        // clean up
        delete dataset;
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


void pgsub_blocking(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer)
{
    vector<Variable*>::const_iterator var_it;

    // read/subset and write each variable...
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        Variable *var = *var_it;
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
            ASSERT(NULL != array);
            writer->write(array, var->get_name());
            delete array;
        }
    }
}


void pgsub_blocking_read_all(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer)
{
    vector<Variable*>::const_iterator var_it;

    // read/subset and write each variable...
    for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
        Variable *var = *var_it;
        Array *array = var->read();
        writer->write(array, var->get_name());
        delete array;
    }
}


void pgsub_nonblocking(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer)
{
    vector<Variable*> record_vars;
    vector<Variable*> nonrecord_vars;
    map<string,Array*> nb_arrays;
    Dimension *udim = dataset->get_udim();
    int64_t nrec = NULL != udim ? udim->get_size() : 0;

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
}


void pgsub_nonblocking_read_all(Dataset *dataset,
        const vector<Variable*> &vars, FileWriter *writer)
{
    vector<Variable*>::const_iterator var_it;
    map<string,Array*> nb_arrays;

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
}


