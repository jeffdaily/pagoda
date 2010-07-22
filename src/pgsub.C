#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdexcept>
#include <string>
#include <vector>

using std::exception;
using std::string;
using std::vector;

#include "Array.H"
#include "Bootstrap.H"
#include "Dataset.H"
#include "Debug.H"
#include "Dimension.H"
#include "FileWriter.H"
#include "Grid.H"
#include "MaskMap.H"
#include "SubsetterCommands.H"
#include "Util.H"
#include "Variable.H"


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

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);
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
        writer->write_atts(dataset->get_atts());
        writer->def_dims(dims);
        writer->def_vars(vars);

        // read/subset and write each variable...
        for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
            Variable *var = *var_it;
            Array *array = var->read();
            writer->write(array, var->get_name());
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
