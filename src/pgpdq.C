#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <algorithm>
#include <cstdlib>
#include <deque>
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
#include "PgpdqCommands.H"
#include "Print.H"
#include "Util.H"
#include "Variable.H"

using std::count;
using std::deque;
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

#define DEBUG 1
#define STR_VEC(vec) pagoda::vec_to_string(vec,",",#vec)

void pgpdq_blocking(PgpdqCommands &cmd);
void pgpdq_blocking_read_all(PgpdqCommands &cmd);
void pgpdq_nonblocking(PgpdqCommands &cmd);
void pgpdq_nonblocking_read_all(PgpdqCommands &cmd);


int main(int argc, char **argv)
{
    PgpdqCommands cmd;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);

        if (cmd.is_reading_all_records()) {
            if (cmd.is_nonblocking()) {
                pgpdq_nonblocking_read_all(cmd);
            } else {
                pgpdq_blocking_read_all(cmd);
            }
        } else {
            if (cmd.is_nonblocking()) {
                pgpdq_nonblocking(cmd);
            } else {
                pgpdq_blocking(cmd);
            }
        }

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


void pgpdq_blocking(PgpdqCommands &cmd)
{
    Dataset *dataset = NULL;
    vector<Variable*> vars;
    vector<Dimension*> dims;
    vector<Attribute*> atts;
    FileWriter *writer = NULL;
    map<string,vector<string> > dims_rdr;
    vector<string> permuted_names = cmd.get_permuted_names();
    string record_dimension = "";

#if DEBUG
    pagoda::println_sync(STR_VEC(permuted_names));
#endif

    cmd.get_inputs(dataset, vars, dims, atts);

    for (size_t i=0,limit=vars.size(); i<limit; ++i) {
        Variable *var = vars[i];
        set<string> rdr_set;
        vector<string> rdr_dim_names;
        vector<Dimension*> var_dims = var->get_dims();
        string var_name = var->get_name();
        deque<size_t> rdr_indices;

        for (size_t j=0,j_limit=var_dims.size(); j<j_limit; ++j) {
            Dimension *dim = var_dims[j];
            string name = dim->get_name();
            ptrdiff_t c = count(permuted_names.begin(), permuted_names.end(), name);
            if (0 == c) {
                // not found
                rdr_dim_names.push_back(name);
            }
            else if (1 == c) {
                rdr_set.insert(name);
                rdr_dim_names.push_back("");
                rdr_indices.push_back(j);
            }
            else {
                // found more than once; an error
                string msg = "reorder dimension found more than once: " + name;
                throw CommandException(msg);
            }
        }

        for (size_t j=0,j_limit=permuted_names.size(); j<j_limit; ++j) {
            if (rdr_set.count(permuted_names[j])) {
                rdr_dim_names[rdr_indices.front()] = permuted_names[j];
                rdr_indices.pop_front();
            }
        }
        dims_rdr[var_name] = rdr_dim_names;
#if DEBUG
        pagoda::println_sync(var_name + ": " + STR_VEC(rdr_dim_names));
#endif
        if (var->has_record()) {
            if (record_dimension.empty()) {
                record_dimension = rdr_dim_names[0];
            }
            else {
                if (record_dimension != rdr_dim_names[0]) {
                    string msg = "dimension reorder list creates duplicate record dimensions: ";
                    msg += record_dimension;
                    msg += " ";
                    msg += rdr_dim_names[0];
                    throw CommandException(msg);
                }
            }
        }
    }

    writer = cmd.get_output();
    writer->write_atts(atts);
    for (size_t i=0,limit=dims.size(); i<limit; ++i) {
        Dimension *dim = dims[i];
        string name = dim->get_name();
        if (name == record_dimension) {
            writer->def_dim(name, -1);
        }
        else {
            writer->def_dim(name, dim->get_size());
        }
    }

    for (size_t i=0,limit=vars.size(); i<limit; ++i) {
        Variable *var = vars[i];
        string name = var->get_name();
        writer->def_var(name, dims_rdr[name], var->get_type(), var->get_atts());
    }

    // clean up
    delete dataset;
    delete writer;
}


void pgpdq_blocking_read_all(PgpdqCommands &cmd)
{
}


void pgpdq_nonblocking(PgpdqCommands &cmd)
{
}


void pgpdq_nonblocking_read_all(PgpdqCommands &cmd)
{
}
