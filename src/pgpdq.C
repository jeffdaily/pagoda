#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdint.h>

#include <algorithm>
#include <cstdlib>
#include <deque>
#include <functional>
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

using std::bind2nd;
using std::count;
using std::deque;
using std::exception;
using std::minus;
using std::string;
using std::map;
using std::transform;
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

#define DEBUG 0
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


#if DEBUG
static ostream& operator << (ostream &os, const map<string,bool> &value)
{
    map<string,bool>::const_iterator it;

    for (it=value.begin(); it!=value.end(); ++it) {
        os << it->first << "->" << it->second << endl;
    }

    return os;
}
#endif


void pgpdq_blocking(PgpdqCommands &cmd)
{
    Dataset *dataset = NULL;
    vector<Variable*> vars;
    vector<Dimension*> dims;
    vector<Attribute*> atts;
    FileWriter *writer = NULL;
    map<string,vector<string> > dims_rdr_names;
    map<string,vector<int64_t> > dims_rdr_indices;
    map<string,vector<int64_t> > dims_rdr_reverse;
    set<string> rdr_vars;
    vector<string> permuted_names = cmd.get_permuted_names();
    map<string,bool> reverse_name;
    string record_dimension = "";

#if DEBUG
    pagoda::println_zero(STR_VEC(permuted_names));
#endif

    for (size_t i=0,limit=permuted_names.size(); i<limit; ++i) {
        if (permuted_names[i][0] == '-') {
            permuted_names[i].erase(0, 1); /* remove '-' */
            reverse_name[permuted_names[i]] = true;
        }
        else {
            reverse_name[permuted_names[i]] = false;
        }
    }

#if DEBUG
    pagoda::println_zero(pagoda::to_string(reverse_name));
#endif

    cmd.get_inputs(dataset, vars, dims, atts);

    for (size_t i=0,limit=vars.size(); i<limit; ++i) {
        Variable *var = vars[i];
        set<string> rdr_set;
        vector<string> rdr_dim_names;
        vector<string> org_dim_names;
        vector<Dimension*> var_dims = var->get_dims();
        string var_name = var->get_name();
        deque<size_t> rdr_indices;
        vector<int64_t> indices;
        vector<int64_t> reverse;

        // determine which indices need to be reordered
        for (size_t j=0,j_limit=var_dims.size(); j<j_limit; ++j) {
            Dimension *dim = var_dims[j];
            string name = dim->get_name();
            ptrdiff_t c = count(permuted_names.begin(), permuted_names.end(), name);
            org_dim_names.push_back(name);
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

        // perform the reordering
        for (size_t j=0,j_limit=permuted_names.size(); j<j_limit; ++j) {
            if (rdr_set.count(permuted_names[j])) {
                rdr_dim_names[rdr_indices.front()] = permuted_names[j];
                rdr_indices.pop_front();
            }
        }

        // check whether this variable truly has reordered dimensions
        // and also generate the reordered dimension indices
        for (size_t j=0,j_limit=var_dims.size(); j<j_limit; ++j) {
            if (org_dim_names[j] != rdr_dim_names[j]
                    || reverse_name[org_dim_names[j]]) {
                size_t k,k_limit;
                rdr_vars.insert(var_name);
                for (k=0,k_limit=var_dims.size(); k<k_limit; ++k) {
                    if (rdr_dim_names[j] == org_dim_names[k]) {
                        indices.push_back(k);
                        if (reverse_name[rdr_dim_names[j]]) {
                            reverse.push_back(-1);
                        }
                        else {
                            reverse.push_back(1);
                        }
                        break;
                    }
                }
                ASSERT(k<k_limit);
            }
            else {
                indices.push_back(j);
                reverse.push_back(1);
            }
        }

        // remember the new (or old) dimension orders
        dims_rdr_names[var_name] = rdr_dim_names;
        dims_rdr_indices[var_name] = indices;
        dims_rdr_reverse[var_name] = reverse;
#if DEBUG
        pagoda::println_zero(var_name + ": " + STR_VEC(rdr_dim_names));
        pagoda::println_zero(string(var_name.size(),' ') + ": " + STR_VEC(indices));
        pagoda::println_zero(string(var_name.size(),' ') + ": " + STR_VEC(reverse));
#endif

        // determine whether the record dimension is changing, and if valid
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

    // get the writer and copy global attributes
    writer = cmd.get_output();
    writer->write_atts(atts);
    
    // define dimensions based on the reordered dimensions
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

    // define variables based on the reordered dimensions
    for (size_t i=0,limit=vars.size(); i<limit; ++i) {
        Variable *var = vars[i];
        string name = var->get_name();
        writer->def_var(name, dims_rdr_names[name],
                var->get_type(), var->get_atts());
    }

    // process each variable
    // for record variables, we go one record at a time
    for (size_t i=0,limit=vars.size(); i<limit; ++i) {
        Variable *var = vars[i];
        string name = var->get_name();
#if DEBUG
        pagoda::println_zero("processing variable: " + name);
#endif
        if (rdr_vars.count(name)) {
            if (var->has_record()) {
                if (record_dimension == var->get_dims()[0]->get_name()) {
                    Array *array = NULL; // reuse allocated array each record
                    Array *result = NULL;
                    vector<int64_t> indices = dims_rdr_indices[name];
                    vector<int64_t> reverse = dims_rdr_reverse[name];
                    indices.erase(indices.begin());
                    reverse.erase(reverse.begin());
                    transform(indices.begin(), indices.end(), indices.begin(),
                            bind2nd(minus<int64_t>(),1));
                    for (int64_t rec=0,limit=var->get_nrec(); rec<limit; ++rec) {
                        array = var->read(rec, array);
                        result = array->transpose(indices, reverse, result);
                        writer->write(result, name, rec);
                    }
                    delete array;
                    delete result;
                }
                else {
                    throw CommandException("record dimension reordering not implemented");
                }
            }
            else {
                Array *array = NULL;
                Array *result = NULL;
                array = var->read();
                ASSERT(NULL != array);
                result = array->transpose(
                        dims_rdr_indices[name], dims_rdr_reverse[name]);
                ASSERT(NULL != result);
                writer->write(result, name);
                delete array;
                delete result;
            }
        }
        else {
            /* no reordering -- this is just like our pgsub code */
            if (var->has_record()) {
                Array *array = NULL; // reuse allocated array each record
                for (int64_t rec=0,limit=var->get_nrec(); rec<limit; ++rec) {
                    array = var->read(rec, array);
                    writer->write(array, name, rec);
                }
                delete array;
            }
            else {
                Array *array = var->read();
                ASSERT(NULL != array);
                writer->write(array, name);
                delete array;
            }
        }
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
