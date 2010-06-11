#ifndef GRID_H_
#define GRID_H_

class Dataset;


/**
 * 
 */
class Grid
{
    public:
        virtual Array* get_cell_lat() = 0;
        virtual Array* get_cell_lon() = 0;
        virtual Array* get_edge_lat() = 0;
        virtual Array* get_edge_lon() = 0;
        virtual Array* get_corner_lat() = 0;
        virtual Array* get_corner_lon() = 0;

        virtual Array* get_cell_cells() = 0;
        virtual Array* get_cell_edges() = 0;
        virtual Array* get_cell_corners() = 0;
        virtual Array* get_edge_cells() = 0;
        virtual Array* get_edge_edges() = 0;
        virtual Array* get_edge_corners() = 0;
        virtual Array* get_corner_cells() = 0;
        virtual Array* get_corner_edges() = 0;
        virtual Array* get_corner_corners() = 0;

        virtual Dataset* get_dataset() = 0;

        friend class Dataset;
};

#endif /* GRID_H_ */
