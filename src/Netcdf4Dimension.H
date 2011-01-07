#ifndef NETCDF4DIMENSION_H_
#define NETCDF4DIMENSION_H_

#include <iostream>
#include <string>

using std::ostream;
using std::string;

#include "Dimension.H"

class Dataset;
class Netcdf4Dataset;


/**
 * The netcdf4 implementation of the Dimension.
 */
class Netcdf4Dimension : public Dimension
{
    public:
        Netcdf4Dimension(Netcdf4Dataset *dataset, int dimid);
        virtual ~Netcdf4Dimension();

        virtual string get_name() const;
        virtual int64_t get_size() const;
        virtual bool is_unlimited() const;
        virtual Dataset* get_dataset() const;

        Netcdf4Dataset* get_netcdf_dataset() const;
        int get_id() const;

        virtual ostream& print(ostream &os) const;

    protected:
        Netcdf4Dataset *dataset;
        int id;
        string name;
        int64_t size;
        bool unlimited;
};

#endif // NETCDF4DIMENSION_H_
