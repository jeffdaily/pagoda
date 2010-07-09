#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <stdexcept>
#include <string>
#include <vector>

using std::exception;
using std::string;
using std::vector;

#include "Aggregation.H"
#include "AggregationJoinExisting.H"
#include "AggregationUnion.H"
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
    Aggregation *agg;
    vector<Variable*> vars;
    vector<Variable*>::iterator var_it;
    vector<Dimension*> dims;
    FileWriter *writer;
    vector<string> infiles;
    vector<Grid*> grids;
    Grid *grid;
    MaskMap *masks;

    try {
        pagoda::initialize(&argc, &argv);

        cmd.parse(argc,argv);
        infiles = cmd.get_input_filenames();
        if (cmd.get_join_name().empty()) {
            dataset = agg = new AggregationUnion;
        } else {
            dataset = agg = new AggregationJoinExisting(cmd.get_join_name());
        }
        for (size_t i=0,limit=infiles.size(); i<limit; ++i) {
            agg->add(Dataset::open(infiles[i]));
        }

        vars = dataset->get_vars();

        pagoda::calculate_required_memory(vars);

        grids = Grid::get_grids(dataset);
        if (grids.empty()) {
            pagoda::print_zero("no grid found\n");
        } else {
            grid = grids[0];
        }
        pagoda::print_zero("grids.size()=%zd\n", grids.size());
        
        masks = new MaskMap(dataset);
        masks->modify(cmd.get_slices());
        masks->modify(cmd.get_box(), grid);

        // read and delete each variable...
        for (var_it=vars.begin(); var_it!=vars.end(); ++var_it) {
            pagoda::print_zero((*var_it)->get_name() + "\n");
            Array *array = (*var_it)->read();
            delete array;
        }

        // clean up
        delete dataset;
        delete masks;

    } catch (exception &ex) {
        pagoda::abort(ex.what());
    }

    return EXIT_SUCCESS;
}
