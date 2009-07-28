#ifndef NETCDFDIMENSION_H_
#define NETCDFDIMENSION_H_

#include <iostream>
    using std::ostream;
#include <string>
    using std::string;

#include "Dimension.H"

class NetcdfDataset;


class NetcdfDimension : public Dimension
{
    public:
        NetcdfDimension(NetcdfDataset *dataset, int dimid);
        virtual ~NetcdfDimension();

        virtual string get_name() const;
        virtual int64_t get_size() const;
        virtual bool is_unlimited() const;

        NetcdfDataset* get_dataset() const;
        int get_id() const;

        virtual ostream& print(ostream &os) const;

    protected:
        NetcdfDataset *dataset;
        int id;
        string name;
        int64_t size;
        bool unlimited;
};

#endif // DIMENSION_H_
