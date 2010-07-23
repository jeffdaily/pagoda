#ifndef GRID_H_
#define GRID_H_

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

using std::ostream;
using std::ostringstream;
using std::runtime_error;
using std::string;
using std::vector;

class Dataset;
class Dimension;
class Variable;


/**
 * A type-safe enumeration of the Grids we handle.
 */
class GridType
{
    public:
        static const GridType GEODESIC;

        GridType(const GridType &type);
        GridType& operator = (const GridType &type);

        bool operator == (const GridType &type) const;
        bool operator != (const GridType &type) const;

        string get_name() const;

        friend ostream& operator << (ostream &os, const GridType &type);

    protected:
        GridType(const string &name);

        static int next_id;
        int id;
        string name;
};


/**
 * Unstructured grid abstraction.
 *
 * Most grids can be represented in an unstructured way with explicit topology
 * and related geometry.
 */
class Grid
{
    public:
        static vector<Grid*> get_grids(const Dataset *dataset);

        virtual ~Grid();

        virtual GridType get_type() const = 0;

        virtual Variable* get_grid() const;

        virtual Variable* get_cell_lat() = 0;
        virtual Variable* get_cell_lon() = 0;
        virtual Variable* get_edge_lat() = 0;
        virtual Variable* get_edge_lon() = 0;
        virtual Variable* get_corner_lat() = 0;
        virtual Variable* get_corner_lon() = 0;
        virtual bool is_radians() = 0;
        virtual bool is_coordinate(const Variable *var);

        virtual Dimension* get_cell_dim() = 0;
        virtual Dimension* get_edge_dim() = 0;
        virtual Dimension* get_corner_dim() = 0;

        virtual Variable* get_cell_cells() = 0;
        virtual Variable* get_cell_edges() = 0;
        virtual Variable* get_cell_corners() = 0;
        virtual Variable* get_edge_cells() = 0;
        virtual Variable* get_edge_edges() = 0;
        virtual Variable* get_edge_corners() = 0;
        virtual Variable* get_corner_cells() = 0;
        virtual Variable* get_corner_edges() = 0;
        virtual Variable* get_corner_corners() = 0;

        virtual bool is_topology(const Variable *var);
        virtual Dimension* get_topology_dim(const Variable *var);

        virtual const Dataset* get_dataset() const = 0;

    protected:
        Grid();
};


/**
 * GridType-based exceptions.
 */
class GridTypeException : public runtime_error
{
    public:
        static string to_string(int value) {
            ostringstream os;
            os << value;
            return os.str();
        }

        explicit GridTypeException(const string &what)
            :   runtime_error(what)
        {}
        explicit GridTypeException(const string &what, int value)
            :   runtime_error(what+string(" :: ")+to_string(value))
        {}
        explicit GridTypeException(const string &what, const GridType &type)
            :   runtime_error(what+string(" :: ")+type.get_name())
        {}
};


/**
 * Grid-based exceptions.
 */
class GridException : public runtime_error
{
    public:
        static string to_string(int value) {
            ostringstream os;
            os << value;
            return os.str();
        }

        explicit GridException(const string &what)
            :   runtime_error(what)
        {}
        explicit GridException(const string &what, int value)
            :   runtime_error(what+string(" :: ")+to_string(value))
        {}
};

#endif /* GRID_H_ */
