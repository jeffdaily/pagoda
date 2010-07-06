/**
 * @file TestMaskMap
 */
#if HAVE_CONFIG_H
#   include <config.h>
#endif

#include <cstdlib>
#include <iomanip>
#include <iostream>
using namespace std;

#include "Array.H"
#include "Bootstrap.H"
#include "DataType.H"
#include "Dimension.H"
#include "Debug.H"
#include "MaskMap.H"
#include "Mask.H"
#include "Slice.H"
#include "Util.H"


class DummyDimension : public Dimension
{
    public:
        DummyDimension(const string& name, int64_t size)
            :   Dimension()
            ,   name(name)
            ,   size(size)
        { }

        virtual ~DummyDimension() { }
        virtual string get_name() const { return name; }
        virtual int64_t get_size() const { return size; }
        virtual bool is_unlimited() const { return false; }
        virtual ostream& print(ostream &os) const {
            return os << "DummyDimension(" << name << "," << size << ")";
        }

    protected:
        string name;
        int64_t size;
};


void dump(const Array *array)
{
    int *data;
    int64_t size = array->get_shape().at(0);

    if (0 != pagoda::me) {
        return;
    }

    data = (int*)array->get(0,size-1);

    for (int64_t i=0; i<size; ++i) {
        cout << setw(3) << i;
    }
    cout << endl;

    for (int64_t i=0; i<size; ++i) {
        cout << setw(3) << data[i];
    }
    cout << endl << endl;
}


int main(int argc, char **argv)
{
    MaskMap *masks = new MaskMap;
    vector<Dimension*> dims;
    DimSlice slice1("dim10", 5, 7, 1);
    DimSlice slice2("dim10,2,4");
    DimSlice slice3("dim10,9");
    vector<DimSlice> slices;

    pagoda::initialize(&argc, &argv);
    pagoda::calculate_required_memory();

    dims.push_back(new DummyDimension("dim10", 10));
    dims.push_back(new DummyDimension("dim50", 50));
    dims.push_back(new DummyDimension("dim100", 100));

    masks->create_masks(dims);

    pagoda::barrier();
    dump(masks->get_mask(dims[0]));

    masks->get_mask(dims[0])->clear();
    masks->get_mask(dims[0])->modify(slice1);
    slices.push_back(slice2);
    slices.push_back(slice3);
    masks->modify(slices);

    pagoda::barrier();
    dump(masks->get_mask(dims[0]));

    pagoda::finalize();

    return EXIT_SUCCESS;
};
