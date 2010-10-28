#ifndef GRID_H_
#define GRID_H_

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "PagodaException.H"

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
        static const GridType REGULAR;

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
 * and related geometry.  Implementing this interface will allow certain
 * operations between grids, whether the underlying grids are unstructured or
 * not.
 *
 * The cell, edge, and corner methods may return Variables and Dimensions not
 * found in the underlying Dataset.  In other words, the Grid interface is
 * allowed to create the missing pieces.  For example, a regular grid is
 * represented using separate latitude and longitude Dimensions.  Calling
 * get_cell_dim() should then return a new Dimension (not found in the
 * original Dataset) with a length of the product of the lengths of the
 * latitude and longitude Dimensions.  This new Dimension represents the
 * Dimension shared between the latitude and longitude coordinate Variables
 * in an unstructured representation of the regular grid.  Likewise,
 * get_cell_lat would not return the lat(lat) coordinate Variable, but rather
 * the lat(cell) auxiliary coordinate Variable in terms of the unstructured
 * representation.
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
class GridTypeException : public PagodaException
{
    public:
        explicit GridTypeException(const string &what)
            :   PagodaException(what)
        {}
        explicit GridTypeException(const string &where, const string &what)
            :   PagodaException(where,what)
        {}
        explicit GridTypeException(const string &where, const string &what,
                                   int value)
            :   PagodaException(where,what,value)
        {}
        explicit GridTypeException(const string &what, int value)
            :   PagodaException("",what,value)
        {}
        explicit GridTypeException(const string &what, const GridType &type)
            :   PagodaException(what+string(" :: ")+type.get_name())
        {}
};


/**
 * Grid-based exceptions.
 */
class GridException : public PagodaException
{
    public:
        explicit GridException(const string &what)
            :   PagodaException(what)
        {}
        explicit GridException(const string &where, const string &what)
            :   PagodaException(where,what)
        {}
        explicit GridException(const string &where, const string &what,
                               int value)
            :   PagodaException(where,what,value)
        {}
};

#endif /* GRID_H_ */
