#include <functional>
    using std::multiplies;
#include <numeric>
    using std::partial_sum;
#include <iostream>
    using std::cout;
    using std::endl;
    using std::ostream;
#include <string>
    using std::string;
#include <vector>
    using std::vector;


ostream& operator << (ostream &os, const vector<string> &v)
{
    if (! v.empty()) os << v[0];
    for (size_t i=1,limit=v.size(); i<limit; ++i) {
        os << ',' << v[i];
    }
    return os;
}


class Base
{
    public:
        Base() {}
        virtual ~Base() {}

        virtual void test() { cout << "Base::test()" << endl; }

        vector<string>& get_stuff() { return stuff; }

    protected:
        vector<string> stuff;
};


class Derived : public Base
{
    public:
        Derived() : Base() {}
        virtual ~Derived() {}

        virtual void test() { cout << "Derived::test()" << endl; }
};


void test(Base *base)
{
    cout << "in function test(Base *base) ";
    base->test();
}


void test(Derived *derived)
{
    cout << "in function test(Derived *derived) ";
    derived->test();
}


int main(int argc, char **argv)
{
    vector<int> v;
    v.push_back(5);
    v.push_back(10);
    v.push_back(15);
    v.push_back(20);
    partial_sum(v.begin(), v.end(), v.begin(), multiplies<int>());
    for (vector<int>::iterator it=v.begin(); it!=v.end(); ++it) {
        cout << *it << endl;
    }


    Base *derived_base = new Derived;
    Base *base = new Base;
    derived_base->test();
    base->test();
    test(derived_base);
    test(base);
    test((Derived*)derived_base);
    test(base);

    cout << "before, stuff = " << base->get_stuff() << endl;
    vector<string> stuff = base->get_stuff();
    stuff.push_back("something");
    cout << " after non-ref, stuff = " << base->get_stuff() << endl;
    vector<string> &stuff2 = base->get_stuff();
    stuff2.push_back("something2");
    cout << " after     ref, stuff = " << base->get_stuff() << endl;
    base->get_stuff().push_back("something3");
    cout << " after get_ref, stuff = " << base->get_stuff() << endl;

    return EXIT_SUCCESS;
}


