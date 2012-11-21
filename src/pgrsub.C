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
#include "PgrsubCommands.H"
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


void pgrsub_blocking(PgrsubCommands &cmd);
void pgrsub_blocking_read_all(PgrsubCommands &cmd);
void pgrsub_nonblocking(PgrsubCommands &cmd);
void pgrsub_nonblocking_read_all(PgrsubCommands &cmd);


int main(int argc, char **argv)
{
    PgrsubCommands cmd;

    try {
        pagoda::initialize(&argc, &argv);
        cmd.parse(argc,argv);

        if (cmd.is_reading_all_records()) {
            if (cmd.is_nonblocking()) {
                pgrsub_nonblocking_read_all(cmd);
            } else {
                pgrsub_blocking_read_all(cmd);
            }
        } else {
            if (cmd.is_nonblocking()) {
                pgrsub_nonblocking(cmd);
            } else {
                pgrsub_blocking(cmd);
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


void pgrsub_blocking(PgrsubCommands &cmd)
{
    Dataset *dataset = NULL;
    vector<Variable*> vars;
    vector<Dimension*> dims;
    vector<Attribute*> atts;
    FileWriter *writer;
    vector<Variable*>::const_iterator var_it;
    Dimension *udim = NULL;
    int64_t nrec = 0;
    vector<string> dim_mask_names;
    vector<Variable*> dim_mask_vars;
    vector<string> dim_mask_dim_names;
    MaskMap *masks = NULL;
    vector<Grid*> grids;
    Grid *grid = NULL;


    cmd.get_inputs(dataset, vars, dims, atts);
    udim = dataset->get_udim();
    nrec = NULL != udim ? udim->get_size() : 0;
    masks = dataset->get_masks();
    grids = dataset->get_grids();
    if (!grids.empty()) {
        grid = grids[0];
    }

    dim_mask_names = cmd.get_dimension_masks();
    // dim masks should be of rank 2, with first dim the record dim
    for (int64_t i=0; i<dim_mask_names.size(); ++i) {
        Variable *var = dataset->get_var(dim_mask_names[i]);
        vector<Dimension*> dims;
        if (NULL == var) {
            throw CommandException("invalid dimension mask -- variable not found");
        }
        dims = var->get_dims();
        if (dims.size() != 2) {
            throw CommandException("invalid dimension mask -- rank != 2");
        }
        if (!dims[0]->is_unlimited()) {
            throw CommandException("invalid dimension mask -- not a record var");
        }
        dim_mask_vars.push_back(var);
        dim_mask_dim_names.push_back(dims[1]->get_name());
    }

    /* for each record, create new output file with a new mask based on the
     * current record index */
    for (int64_t rec=0; rec<nrec; ++rec) {
        bool skip_zero = false;
        for (int64_t i=0; i<dim_mask_vars.size(); ++i) {
            Variable *var = dim_mask_vars[i];
            Array *array = NULL;
            dataset->push_masks(NULL);
            array = var->read(rec);
            (void)dataset->pop_masks();
            masks->clear_mask(dim_mask_dim_names[i]);
            masks->modify(dim_mask_dim_names[i], array);
            if (array->get_count() == 0) {
                skip_zero = true;
            }
            delete array;
        }
        if (skip_zero) {
            continue;
        }
        writer = cmd.get_output_at(rec);
        writer->write_atts(atts);
        writer->def_dims(dims);
        writer->def_vars(vars);

        // read/subset and write each variable...
        for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
            Variable *var = *var_it;
            if (var->has_record()) {
                Array *array = var->read(rec);
                ASSERT(NULL != array);
                writer->write(array, var->get_name(), 0);
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
        delete writer;
    }

    // clean up
    delete dataset;
}


void pgrsub_blocking_read_all(PgrsubCommands &cmd)
{
    throw CommandException("blocking read all not implemented");
}


void pgrsub_nonblocking(PgrsubCommands &cmd)
{
    throw CommandException("nonblocking not implemented");
}


void pgrsub_nonblocking_read_all(PgrsubCommands &cmd)
{
    throw CommandException("nonblocking read all not implemented");
}
