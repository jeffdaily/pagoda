#ifndef NETCDF4DATASET_H_
#define NETCDF4DATASET_H_

#include <netcdf.h>

#include <string>
#include <vector>

using std::string;
using std::vector;

#include "AbstractDataset.H"
#include "FileFormat.H"

class Array;
class Attribute;
class Dimension;
class Mask;
class Netcdf4Attribute;
class Netcdf4Dimension;
class Netcdf4Variable;
class Variable;

Dataset* pagoda_netcdf4_open(const string &filename);

/**
 * The netcdf4 implementation of the Dataset.
 *
 * A Netcdf4Dataset instance owns the Variable, Dimension, and Attribute
 * instances which are accessed via its methods.  Do not explicitly delete
 * them.
 */
class Netcdf4Dataset : public AbstractDataset
{
    public:
        static DataType to_dt(const nc_type &type);

        /**
         * Constructs a Netcdf4Dataset by opening the given filename.
         *
         * @param[in] filename to open
         */
        Netcdf4Dataset(const string &filename);

        /**
         * Destructs and automatically closes the Netcdf4Dataset.
         */
        virtual ~Netcdf4Dataset();

        virtual void close();

        virtual vector<Attribute*> get_atts() const;
        virtual vector<Dimension*> get_dims() const;
        virtual vector<Variable*> get_vars() const;

        virtual void wait();

        virtual FileFormat get_file_format() const;

        virtual ostream& print(ostream &os) const;

        virtual Attribute* get_att(const string &name,
                                   bool ignore_case=false,
                                   bool within=false) const;
        virtual Attribute* get_att(const vector<string> &names,
                                   bool ignore_case=false,
                                   bool within=false) const;

        /**
         * Returns the Netcdf4Attribute at index i.
         *
         * @param[in] i the index
         * @return the Netcdf4Attribute at index i
         */
        Netcdf4Attribute* get_att(size_t i) const;

        virtual Dimension* get_dim(const string &name,
                                   bool ignore_case=false,
                                   bool within=false) const;

        /**
         * Returns the Netcdf4Dimension at index i.
         *
         * @param[in] i the index
         * @return the Netcdf4Dimension at index i
         */
        Netcdf4Dimension* get_dim(size_t i) const;

        virtual Variable* get_var(const string &name,
                                  bool ignore_case=false,
                                  bool within=false) const;

        /**
         * Returns the Netcdf4Variable at index i.
         *
         * @param[in] i the index
         * @return the Netcdf4Variable at index i
         */
        Netcdf4Variable* get_var(size_t i) const;

        /**
         * Returns the name of the file represented by this Dataset.
         *
         * @return the name of the file represented by this Dataset
         */
        string get_filename() const;

        /**
         * Returns the "ncid" aka the NetCDF ID for this Dataset.
         *
         * @return the "ncid" aka the NetCDF ID for this Dataset
         */
        int get_id() const;

        friend class Netcdf4Variable;

    protected:
        string filename; /**< name of the file opened */
        int ncid; /**< NetCDF handle for the opened file */
        int udim; /**< NetCDF handle for the unlimited (aka record) dim */
        vector<Netcdf4Attribute*> atts; /**< all attributes */
        vector<Netcdf4Dimension*> dims; /**< all dimensions */
        vector<Netcdf4Variable*> vars; /**< all variables */
        bool is_open; /**< whether this Dataset is open */
};

#endif // NETCDF4DATASET_H_
