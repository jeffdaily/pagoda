#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <cassert>
#include <cstdlib>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "Array.H"
#include "Bootstrap.H"
#include "CommandException.H"
#include "CommandLineOption.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "PgeaCommands.H"
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


int main(int argc, char **argv)
{
    PgeaCommands cmd;
    vector<Dataset*> datasets;
    vector<vector<Variable*> > all_vars;
    vector<vector<Dimension*> > all_dims;
    FileWriter *writer = NULL;
    vector<Grid*> grids;
    vector<MaskMap*> masks;
    string op;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);

        if (cmd.is_helping()) {
            pagoda::print_zero(cmd.get_usage());
            pagoda::finalize();
            return EXIT_SUCCESS;
        }

        datasets = cmd.get_datasets();
        all_vars = cmd.get_variables(datasets);
        all_dims = cmd.get_dimensions(datasets);
        op = cmd.get_operator();

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
        writer->def_dims(all_dims.at(0));
        writer->def_vars(all_vars.at(0));

        // for each variable in the first set, read it, then find the
        // associated variables from the other sets and read them
        for (size_t i=0; i<all_vars[0].size(); ++i) {
            Variable *var = all_vars[0][i];
            Array *result = NULL;
            Array *array = NULL;

            // read the first var directly into result
            result = var->read(result);
            if (op == OP_RMS || op == OP_RMSSDN || op == OP_AVGSQR) {
                // square the values
                result->imul(result);
            }

            // read the same variables from the other datasets
            for (size_t j=1; j<all_vars.size(); ++j) {
                Variable *current_var = NULL;

                // locate the variable
                current_var = Variable::find(all_vars[j], var);
                assert(current_var != NULL);

                // read it in
                array = current_var->read(array);
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
                    // sum the values or squares
                    result->iadd(array);
                }
            }

            // normalize, multiply, etc where necessary
            if (op == OP_AVG || op == OP_SQRT || op == OP_SQRAVG
                    || op == OP_RMS || op == OP_RMS || op == OP_AVGSQR) {
                ScalarArray scalar(result->get_type());
                scalar.fill_value(datasets.size());
                result->idiv(&scalar);
            }
            else if (op == OP_RMSSDN) {
                ScalarArray scalar(result->get_type());
                scalar.fill_value(datasets.size()-1);
                result->idiv(&scalar);
            }

            // some operations require additional process */
            if (op == OP_RMS || op == OP_RMSSDN || op == OP_SQRT) {
                result->ipow(0.5);
            }
            else if (op == OP_SQRAVG) {
                result->imul(result);
            }

            writer->write(result, var->get_name());
            delete result;
            delete array;
        }

        // clean up
        for (size_t i=0; i<datasets.size(); ++i) {
            delete datasets[i];
            delete masks[i];
        }
        delete writer;

        pagoda::finalize();

    }
    catch (CommandException &ex) {
        for (size_t i=0; i<datasets.size(); ++i) {
            delete datasets[i];
            delete masks[i];
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
