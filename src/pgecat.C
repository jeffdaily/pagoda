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
#include "PgecatCommands.H"
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


void pgecat_blocking(PgecatCommands &cmd);
void pgecat_blocking_read_all(PgecatCommands &cmd);
void pgecat_nonblocking(PgecatCommands &cmd);
void pgecat_nonblocking_read_all(PgecatCommands &cmd);


int main(int argc, char **argv)
{
    PgecatCommands cmd;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);

        if (cmd.is_reading_all_records()) {
            if (cmd.is_nonblocking()) {
                pgecat_nonblocking_read_all(cmd);
            } else {
                pgecat_blocking_read_all(cmd);
            }
        } else {
            if (cmd.is_nonblocking()) {
                pgecat_nonblocking(cmd);
            } else {
                pgecat_blocking(cmd);
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


void pgecat_blocking(PgecatCommands &cmd)
{
    vector<Dataset*> datasets;
    vector<vector<Variable*> > all_vars;
    vector<vector<Dimension*> > all_dims;
    FileWriter *writer = NULL;
    vector<Grid*> grids;
    vector<MaskMap*> masks;

    datasets = cmd.get_datasets();
    all_vars = cmd.get_variables(datasets);
    all_dims = cmd.get_dimensions(datasets);

    // get grids, one per dataset
    for (size_t i=0; i<datasets.size(); ++i) {
        grids.push_back(datasets[i]->get_grid());
        if (grids.back() == NULL) {
            ostringstream str;
            str << "no grid found in dataset " << i;
            pagoda::println_zero(str.str());
        }
    }

    // we can't share a MaskMap in case the record dimensions differ
    // between ensembles
    for (size_t i=0; i<datasets.size(); ++i) {
        masks.push_back(new MaskMap(datasets[i]));
        masks[i]->modify(cmd.get_index_hyperslabs());
        masks[i]->modify(cmd.get_coord_hyperslabs(), grids[i]);
        masks[i]->modify(cmd.get_boxes(), grids[i]);
    }

    // sanity check that all datasets are the same
    // can't compare Datasets directly because they don't reflect a
    // possible subset of Variables or Dimensions
    // must be performed after masks are initialized otherwise, for
    // example, the record dimenions might be different sizes since we
    // allow ensembles to have different record lengths
    for (size_t i=1; i<all_dims.size(); ++i) {
        if (!Dimension::equal(all_dims[0],all_dims[i])) {
            ERRCODE("dimensions mismatch", i);
        }
    }
    for (size_t i=1; i<all_vars.size(); ++i) {
        if (!Variable::equal(all_vars[0],all_vars[i])) {
            ERRCODE("variables mismatch", i);
        }
    }

    // writer is defined based on the first dataset in the ensemble
    writer = cmd.get_output();
    writer->write_atts(cmd.get_attributes(datasets.at(0)));

    // define dimensions
    for (size_t i=0,limit=all_dims[0].size(); i<limit; ++i) {
        Dimension *dim = all_dims[0][i];
        if (dim->is_unlimited()) {
            writer->def_dim(dim->get_name(), dim->get_size());
        }
        else {
            writer->def_dim(dim);
        }
    }
    writer->def_dim(cmd.get_unlimited_name(), datasets.size());
    
    // define variables
    for (size_t i=0,limit=all_vars[0].size(); i<limit; ++i) {
        Variable *var = all_vars[0][i];
        if (var->is_grid()) {
            writer->def_var(var);
        }
        else {
            vector<Dimension*> dims;
            vector<Dimension*>::const_iterator dim_it;
            vector<string> dim_names;

            dim_names.reserve(var->get_ndim()+1);
            dim_names.push_back(cmd.get_unlimited_name());
            dims = var->get_dims();
            for (dim_it=dims.begin(); dim_it!=dims.end(); ++dim_it) {
                dim_names.push_back((*dim_it)->get_name());
            }
            writer->def_var(var->get_name(), dim_names, var->get_type(),
                    var->get_atts());
        }
    }

    // for each Dataset, process each Variable
    for (size_t i=0,i_limit=datasets.size(); i<i_limit; ++i) {
        Dataset *dataset = datasets[i];
        for (size_t j=0,j_limit=all_vars[i].size(); j<j_limit; ++j) {
            Variable *var = all_vars[i][j];
            if (var->is_grid()) {
                if (0 == i) {
                    // we are the first dataset and we found a grid variable
                    // copy the variable to the output
                    Array *array = var->read();
                    ASSERT(NULL != array);
                    writer->write(array, var->get_name());
                    delete array;
                }
            }
            else {
                if (var->has_record()) {
                    Array *array = NULL; // reuse allocated array each record
                    for (int64_t r=0,r_limit=var->get_nrec(); r<r_limit; ++r) {
                        array = var->read(r, array);
                        writer->write(array, var->get_name(), i, r);
                    }
                    delete array;
                }
                else {
                    Array *array = var->read();
                    ASSERT(NULL != array);
                    writer->write(array, var->get_name(), i);
                    delete array;
                }
            }
        }
    }

    // clean up
    for (size_t i=0,limit=datasets.size(); i<limit; ++i) {
        delete datasets[i];
        delete masks[i];
    }
    delete writer;
}


void pgecat_blocking_read_all(PgecatCommands &cmd)
{
}


void pgecat_nonblocking(PgecatCommands &cmd)
{
}


void pgecat_nonblocking_read_all(PgecatCommands &cmd)
{
}
