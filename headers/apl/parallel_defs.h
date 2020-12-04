#ifndef __PARALLEL_DEFS_H__
#define __PARALLEL_DEFS_H__

#include <cassert>
#include <limits>
#include <utility>
#include <tuple>
#include "mpi.h"

#ifndef NDEBUG
#define apl_MPI_CHECKER(func) \
do \
{ \
    int ret = MPI_SUCCESS; \
    ret = func; \
    assert(ret == MPI_SUCCESS && #func); \
} \
while(0)
#else
#define apl_MPI_CHECKER(func) (void)func
#endif

namespace apl
{

    typedef int process;

    enum class TAG
    {
        UNDEFINED = std::numeric_limits<int>::max(),
        ANY = MPI_ANY_TAG,
        MAIN = 0,
        SIZE = 1
    };

    template<size_t... Indexes, typename Func, typename... Args>
    auto apply_impl(std::index_sequence<Indexes...>, Func&& f, const std::tuple<Args...>& args)
    { return f(std::get<Indexes>(args)...); }

    template<size_t... Indexes, typename Func, typename... Args>
    auto apply_impl(std::index_sequence<Indexes...>, Func&& f, std::tuple<Args...>& args)
    { return f(std::get<Indexes>(args)...); }

    template<typename Func, typename... Args>
    auto apply(Func&& f, const std::tuple<Args...>& args)
    { return apply_impl(std::index_sequence_for<Args...>(), std::forward<Func>(f), args); }

    template<typename Func, typename... Args>
    auto apply(Func&& f, std::tuple<Args...>& args)
    { return apply_impl(std::index_sequence_for<Args...>(), std::forward<Func>(f), args); }

    template<typename Type, typename Member>
    size_t offset_of(Member Type::* member)
    { return reinterpret_cast<char*>(&(reinterpret_cast<Type*>(0)->*member)) - reinterpret_cast<char*>(0); }

}

#endif // __PARALLEL_DEFS_H__
