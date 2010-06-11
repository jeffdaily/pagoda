#include <iostream>
using namespace std;

class Base {};
class Derived : public Base {};

void somefunc(Base *obj)
{
    cout << "somefunc Base" << endl;
}
void somefunc(Derived *obj)
{
    cout << "somefunc Derived" << endl;
}

int main(int argc, char **argv)
{
    Base base, *base_ptr;
    Derived derived, *derived_ptr;

    somefunc(&base);
    somefunc(&derived);

    base_ptr = &derived;
    somefunc(base_ptr);
    somefunc((Derived*)base_ptr);

    return 0;
}
