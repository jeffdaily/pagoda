#ifndef GEOGRID_H_
#define GEOGRID_H_

#include "Grid.H"

class Dataset;
class Dimension;
class Variable;


/**
 * The geodesic grid implementation of the Grid interface.
 */
class GeoGrid : public Grid
{
    public:
        static vector<Grid*> get_grids(const Dataset *dataset);

        GeoGrid(const Dataset *dataset, const Variable *grid_variable=NULL);
        virtual ~GeoGrid();

        virtual GridType get_type() const;

        virtual Variable* get_cell_lat();
        virtual Variable* get_cell_lon();
        virtual Variable* get_edge_lat();
        virtual Variable* get_edge_lon();
        virtual Variable* get_corner_lat();
        virtual Variable* get_corner_lon();
        virtual bool is_radians();

        virtual Dimension* get_cell_dim();
        virtual Dimension* get_edge_dim();
        virtual Dimension* get_corner_dim();

        virtual Variable* get_cell_cells();
        virtual Variable* get_cell_edges();
        virtual Variable* get_cell_corners();
        virtual Variable* get_edge_cells();
        virtual Variable* get_edge_edges();
        virtual Variable* get_edge_corners();
        virtual Variable* get_corner_cells();
        virtual Variable* get_corner_edges();
        virtual Variable* get_corner_corners();

        virtual const Dataset* get_dataset() const;

    protected:
        GeoGrid();
        GeoGrid(const GeoGrid &that);

        Variable* get_coord(const string &att_name,
                            const string &coord_name, const string &dim_name);
        Dimension* get_dim(const string &att_name, const string &dim_name);
        Variable* get_topology(const string &att_name, const string &var_name);

        const Dataset *dataset;
        const Variable *grid_var;
};

#endif /* GEOGRID_H_ */
