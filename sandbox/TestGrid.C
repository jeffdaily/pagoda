#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <cstdlib>
#include <map>

#include "Array.H"
#include "Bootstrap.H"
#include "Dataset.H"
#include "Debug.H"
#include "Grid.H"

using std::map;


typedef Variable* (Grid::*gridvar_t)(void);
typedef map<string,gridvar_t> gridvar_m;
typedef Dimension* (Grid::*griddim_t)(void);
typedef map<string,griddim_t> griddim_m;


int main(int argc, char **argv)
{
    Dataset *dataset;
    vector<Grid*> grids;
    vector<Grid*>::iterator grid_it;
    gridvar_m var_funcs;
    gridvar_m::iterator var_func_it;
    griddim_m dim_funcs;
    griddim_m::iterator dim_func_it;

    var_funcs["get_cell_lat"]       = &Grid::get_cell_lat;
    var_funcs["get_cell_lon"]       = &Grid::get_cell_lon;
    var_funcs["get_edge_lat"]       = &Grid::get_edge_lat;
    var_funcs["get_edge_lon"]       = &Grid::get_edge_lon;
    var_funcs["get_corner_lat"]     = &Grid::get_corner_lat;
    var_funcs["get_corner_lon"]     = &Grid::get_corner_lon;
    var_funcs["get_cell_cells"]     = &Grid::get_cell_cells;
    var_funcs["get_cell_edges"]     = &Grid::get_cell_edges;
    var_funcs["get_cell_corners"]   = &Grid::get_cell_corners;
    var_funcs["get_edge_cells"]     = &Grid::get_edge_cells;
    var_funcs["get_edge_edges"]     = &Grid::get_edge_edges;
    var_funcs["get_edge_corners"]   = &Grid::get_edge_corners;
    var_funcs["get_corner_cells"]   = &Grid::get_corner_cells;
    var_funcs["get_corner_edges"]   = &Grid::get_corner_edges;
    var_funcs["get_corner_corners"] = &Grid::get_corner_corners;

    dim_funcs["get_cell_dim"]       = &Grid::get_cell_dim;
    dim_funcs["get_edge_dim"]       = &Grid::get_edge_dim;
    dim_funcs["get_corner_dim"]     = &Grid::get_corner_dim;

    pagoda::initialize(&argc, &argv);

    if (2 != argc) {
        pagoda::print_zero("Usage: TestGrid filename\n");
        pagoda::finalize();
        return EXIT_FAILURE;
    }

    dataset = Dataset::open(argv[1]);
    grids = dataset->get_grids();

    if (grids.empty()) {
        pagoda::print_zero("no grids found\n");
    }

    for (grid_it=grids.begin(); grid_it!=grids.end(); ++grid_it) {
        Grid *grid = *grid_it;

        pagoda::print_zero("Found grid: " + grid->get_type().get_name() + "\n");
        for (var_func_it=var_funcs.begin();
                var_func_it!=var_funcs.end(); ++var_func_it) {
            string name = var_func_it->first;
            gridvar_t func = var_func_it->second;
            Variable *var = (grid->*func)();
            if (NULL != var) {
                pagoda::print_zero("found\t");
            } else {
                pagoda::print_zero("missing\t");
            }
            pagoda::print_zero(name + "\n");
        }

        for (dim_func_it=dim_funcs.begin();
                dim_func_it!=dim_funcs.end(); ++dim_func_it) {
            string name = dim_func_it->first;
            griddim_t func = dim_func_it->second;
            Dimension *dim = (grid->*func)();
            if (NULL != dim) {
                pagoda::print_zero("found\t");
            } else  {
                pagoda::print_zero("missing\t");
            }
            pagoda::print_zero(name + "\n");
        }
    }

    delete dataset;

    pagoda::finalize();

    return EXIT_SUCCESS;
}
