#if HAVE_CONFIG_H
#   include <config.h>
#endif

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
#include "PgraCommands.H"
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
int F77_DUMMY_MAIN() { return 1; }
#endif


int main(int argc, char **argv)
{
    PgraCommands cmd;
    Dataset *dataset = NULL;
    vector<Variable*> vars;
    vector<Variable*>::iterator var_it;
    vector<Dimension*> dims;
    FileWriter *writer = NULL;
    vector<Grid*> grids;
    Grid *grid = NULL;
    MaskMap *masks = NULL;
    string op;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);

        if (cmd.is_helping()) {
            pagoda::print_zero(cmd.get_usage());
            pagoda::finalize();
            return EXIT_SUCCESS;
        }

        dataset = cmd.get_dataset();
        vars = cmd.get_variables(dataset);
        dims = cmd.get_dimensions(dataset);
        op = cmd.get_operator();

        grids = dataset->get_grids();
        if (grids.empty()) {
            pagoda::print_zero("no grid found\n");
        } else {
            grid = grids[0];
        }
        
        masks = new MaskMap(dataset);
        masks->modify(cmd.get_slices());
        masks->modify(cmd.get_boxes(), grid);

        writer = cmd.get_output();
        if (!cmd.is_appending()) {
            writer->write_atts(cmd.get_attributes(dataset));
            writer->def_dims(dims);
            writer->def_vars(vars);
        }

        // read each variable in order
        // for record variables, read one record at a time
        for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
            Variable *var = *var_it;
            if (var->has_record() && var->get_nrec() > 0) {
                int64_t nrec = var->get_nrec();
                Array *result = NULL;
                Array *array = NULL; // reuse allocated array each record

                // optimization: read the first record directly into result
                result = var->read(0, result);
                if (op == OP_RMS || op == OP_RMSSDN || op == OP_AVGSQR) {
                    // square the values
                    result->imul(result);
                }

                // read the rest of the records
                for (int64_t rec=1; rec<nrec; ++rec) {
                    array = var->read(rec, array);
                    if (op == OP_MAX || op == OP_MIN) {
                        if (op == OP_MAX) {
                            result->imax(array);
                        } else {
                            result->imin(array);
                        }
                    } else {
                        if (op == OP_RMS
                                || op == OP_RMSSDN
                                || op == OP_AVGSQR) {
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
                    scalar.fill_value(nrec);
                    result->idiv(&scalar);
                } else if (op == OP_RMSSDN) {
                    ScalarArray scalar(result->get_type());
                    scalar.fill_value(nrec-1);
                    result->idiv(&scalar);
                }

                // some operations require additional process */
                if (op == OP_RMS || op == OP_RMSSDN || op == OP_SQRT) {
                    result->ipow(0.5);
                } else if (op == OP_SQRAVG) {
                    result->imul(result);
                }

                writer->write(result, var->get_name(), 0);
                delete result;
                delete array;
            } else {
                Array *array = var->read();
                writer->write(array, var->get_name());
                delete array;
            }
        }

        // clean up
        delete dataset;
        delete masks;
        delete writer;

        pagoda::finalize();

    } catch (CommandException &ex) {
        if (dataset) {
            delete dataset;
        }
        if (masks) {
            delete masks;
        }
        if (writer) {
            delete writer;
        }
        pagoda::println_zero(ex.what());
        pagoda::finalize();
        return EXIT_FAILURE;
    } catch (exception &ex) {
        pagoda::abort(ex.what());
    }

    return EXIT_SUCCESS;
}
