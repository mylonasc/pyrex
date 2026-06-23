#include <pybind11/pybind11.h>

#include "bindings.hpp"

PYBIND11_MODULE(_pyrex, m) {
    bind_pyrex(m);
}
