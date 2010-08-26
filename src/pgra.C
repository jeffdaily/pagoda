#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "Array.H"
#include "Bootstrap.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "ScalarArray.H"
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
int F77_DUMMY_MAIN() { return 1; }
#endif


int main(int argc, char **argv)
{
    SubsetterCommands cmd;
    Dataset *dataset;
    vector<Variable*> vars;
    vector<Variable*>::iterator var_it;
    vector<Dimension*> dims;
    FileWriter *writer;
    vector<Grid*> grids;
    Grid *grid;
    MaskMap *masks;
#if defined(READ_NONBLOCKING)
    map<string,Array*> arrays;
    map<string,Array*>::iterator array;
#endif

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);

        if (cmd.get_help()) {
            pagoda::print_zero(cmd.get_usage());
            pagoda::finalize();
            return EXIT_SUCCESS;
        }

        dataset = cmd.get_dataset();
        vars = cmd.get_variables(dataset);
        dims = cmd.get_dimensions(dataset);

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
        writer->write_atts(cmd.get_attributes(dataset));
        writer->def_dims(dims);
        writer->def_vars(vars);

        // read each variable in order
        // for record variables, read one record at a time
        for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
            Variable *var = *var_it;
            if (var->has_record() && var->get_nrec() > 0) {
                int64_t nrec = var->get_nrec();
                ScalarArray nrec_array(var->get_type());
                Array *result = NULL;
                Array *array = NULL; // reuse allocated array each record

                nrec_array.fill(nrec);
                result = var->read(0, result);
                result->idiv(&nrec_array);
                for (int64_t rec=1; rec<nrec; ++rec) {
                    array = var->read(rec, array);
                    array->idiv(&nrec_array);
                    result->iadd(array);
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

    } catch (exception &ex) {
        pagoda::abort(ex.what());
    }

    return EXIT_SUCCESS;
}
