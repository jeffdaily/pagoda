#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <map>

using std::map;

#include "Array.H"
#include "Bootstrap.H"
#include "Dataset.H"
#include "Debug.H"
#include "Grid.H"


typedef Variable* (Grid::*gridfunc_t)(void);
typedef map<string,gridfunc_t> gridfunc_m;


int main(int argc, char **argv)
{
    Dataset *dataset;
    vector<Grid*> grids;
    vector<Grid*>::iterator grid_it;
    gridfunc_m funcs;
    gridfunc_m::iterator func_it;

    funcs["get_cell_lat"]       = &Grid::get_cell_lat;
    funcs["get_cell_lon"]       = &Grid::get_cell_lon;
    funcs["get_edge_lat"]       = &Grid::get_edge_lat;
    funcs["get_edge_lon"]       = &Grid::get_edge_lon;
    funcs["get_corner_lat"]     = &Grid::get_corner_lat;
    funcs["get_corner_lon"]     = &Grid::get_corner_lon;
    funcs["get_cell_cells"]     = &Grid::get_cell_cells;
    funcs["get_cell_edges"]     = &Grid::get_cell_edges;
    funcs["get_cell_corners"]   = &Grid::get_cell_corners;
    funcs["get_edge_cells"]     = &Grid::get_edge_cells;
    funcs["get_edge_edges"]     = &Grid::get_edge_edges;
    funcs["get_edge_corners"]   = &Grid::get_edge_corners;
    funcs["get_corner_cells"]   = &Grid::get_corner_cells;
    funcs["get_corner_edges"]   = &Grid::get_corner_edges;
    funcs["get_corner_corners"] = &Grid::get_corner_corners;

    pagoda::initialize(&argc, &argv);

    if (2 != argc) {
        pagoda::print_zero("Usage: TestGrid filename\n");
        pagoda::finalize();
        return EXIT_FAILURE;
    }

    dataset = Dataset::open(argv[1]);
    grids = Grid::get_grids(dataset);

    for (grid_it=grids.begin(); grid_it!=grids.end(); ++grid_it) {
        Grid *grid = *grid_it;

        pagoda::print_zero("Found grid: " + grid->get_type().get_name() + "\n");
        for (func_it=funcs.begin(); func_it!=funcs.end(); ++func_it) {
            Variable *var = NULL;
            string name = func_it->first;
            gridfunc_t func = func_it->second;
            if ((var = (grid->*func)())) {
                pagoda::print_zero("found\t");
            }
            else {
                pagoda::print_zero("missing\t");
            }
            pagoda::print_zero(name + "\n");
        }
    }

    delete dataset;

    pagoda::finalize();

    return EXIT_SUCCESS;
}
